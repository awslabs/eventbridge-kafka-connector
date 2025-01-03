/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static com.jayway.jsonpath.Configuration.defaultConfiguration;
import static com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static software.amazon.awssdk.core.async.AsyncRequestBody.fromString;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.panic;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.retry;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;
import static software.amazon.event.kafkaconnector.cache.MessageDigestCacheKey.sha512Of;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.slf4j.Logger;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.cache.Cache;
import software.amazon.event.kafkaconnector.cache.FifoCache;
import software.amazon.event.kafkaconnector.cache.MessageDigestCacheKey;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.mapping.EventBridgeMappingResult;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;
import software.amazon.event.kafkaconnector.util.ThrowingFunction;
import software.amazon.event.kafkaconnector.util.ThrowingFunctionApplyException;

public class S3EventBridgeEventDetailValueOffloading
    implements EventBridgeEventDetailValueOffloadingStrategy {

  public static final String JSON_PATH_PREFIX = "$.detail.value";

  private static final int SDK_TIMEOUT = 5000; // timeout in milliseconds for SDK calls

  // maximum size of cache (by Map.Entry<MessageDigestCacheKey, UUID>) => ~16MiB
  // size of each Map.Entry<MessageDigestCacheKey, UUID> is 168Byte (for x86_64)
  private static final int MAX_CACHE_SIZE = 100_000;

  private static final Logger logger =
      ContextAwareLoggerFactory.getLogger(S3EventBridgeEventDetailValueOffloading.class);

  private static final Configuration jsonPathConfiguration =
      defaultConfiguration()
          .addOptions(
              // suppress exception otherwise com.jayway.jsonpath.ReadContext#read throws an
              // exception if JSON path could not be found
              SUPPRESS_EXCEPTIONS);

  private final String bucketName;
  private final S3AsyncClient client;

  private final Cache<MessageDigestCacheKey, UUID> s3ObjectKeyCache;
  private final Supplier<UUID> idGenerator;

  private static final JsonPath jsonPathAdd = JsonPath.compile("$");

  private final String jsonPathExp;
  private final JsonPath jsonPathRemove;

  public S3EventBridgeEventDetailValueOffloading(
      final S3AsyncClient client,
      final String bucketName,
      final String jsonPathExp,
      final Cache<MessageDigestCacheKey, UUID> s3ObjectKeyCache,
      final Supplier<UUID> idGenerator) {

    this.bucketName = bucketName;
    this.client = client;

    this.s3ObjectKeyCache = s3ObjectKeyCache;
    this.idGenerator = idGenerator;

    this.jsonPathExp = jsonPathExp;
    this.jsonPathRemove = compileRestrictedJsonPath(jsonPathExp);
  }

  public S3EventBridgeEventDetailValueOffloading(
      final S3AsyncClient client, final String bucketName, final String jsonPathExp) {

    this.bucketName = bucketName;
    this.client = client;

    this.s3ObjectKeyCache = new FifoCache<>(MAX_CACHE_SIZE);
    this.idGenerator = UUID::randomUUID;

    this.jsonPathExp = jsonPathExp;
    this.jsonPathRemove = compileRestrictedJsonPath(jsonPathExp);
  }

  private static JsonPath compileRestrictedJsonPath(String value) {
    JsonPath jsonPath;
    try {
      jsonPath = JsonPath.compile(value);
    } catch (Exception e) {
      throw new IllegalArgumentException(format("Invalid JSON Path '%s'.", value));
    }

    if (!jsonPath.getPath().startsWith("$['detail']['value']")) {
      throw new IllegalArgumentException(
          format("JSON Path must start with '%s' but is '%s'.", JSON_PATH_PREFIX, value));
    }
    if (!jsonPath.isDefinite()) {
      throw new IllegalArgumentException(
          format("JSON Path must be definite but '%s' is not.", value));
    }
    // rewrite $.detail -> $,
    // because PutEventsRequestEntry#detail is the sub document of $.detail
    return JsonPath.compile("$" + jsonPath.getPath().substring("$['detail']".length()));
  }

  public static void validateJsonPath(String jsonPathExp) {
    compileRestrictedJsonPath(jsonPathExp);
  }

  @Override
  public EventBridgeMappingResult apply(
      final List<MappedSinkRecord<PutEventsRequestEntry>> putEventsRequestEntries) {

    var result =
        putEventsRequestEntries.stream()
            .map(this::apply)
            .collect(partitioningBy(EventBridgeResult::isSuccess));

    var success = result.get(true).stream().map(EventBridgeResult::success).collect(toList());
    var errors = result.get(false).stream().map(EventBridgeResult::failure).collect(toList());

    return new EventBridgeMappingResult(success, errors);
  }

  private EventBridgeResult<PutEventsRequestEntry> apply(
      final MappedSinkRecord<PutEventsRequestEntry> item) {

    var sinkRecord = item.getSinkRecord();
    var putEventsRequestEntry = item.getValue();

    try {
      var documentContext = JsonPath.parse(putEventsRequestEntry.detail(), jsonPathConfiguration);

      var value = documentContext.read(jsonPathRemove);
      if (value != null) {
        var payload =
            value instanceof Map || value instanceof List
                ? JsonPath.parse(value).jsonString()
                : value.toString();
        documentContext
            .delete(jsonPathRemove)
            .put(jsonPathAdd, "dataref", s3ArnOf(putS3Object(payload)))
            .put(jsonPathAdd, "datarefJsonPath", jsonPathExp);
      }

      return success(
          sinkRecord, putEventsRequestEntry.copy(it -> it.detail(documentContext.jsonString())));

    } catch (final ThrowingFunctionApplyException e) {
      var unwrapped = e.getCause();
      if (unwrapped instanceof ExecutionException
          || unwrapped instanceof InterruptedException
          || unwrapped instanceof TimeoutException) {

        var cause = unwrapped.getCause();
        if (cause instanceof S3Exception && ((S3Exception) cause).statusCode() < 500) {
          return failure(sinkRecord, panic(e));
        }
        return failure(sinkRecord, retry(e));
      }
      return failure(sinkRecord, panic(unwrapped));
    }
  }

  private String s3ArnOf(UUID uuid) {
    return format("arn:aws:s3:::%s/%s", bucketName, uuid);
  }

  private UUID putS3Object(final String payload) {
    var durationHolder = new Duration[] {null};
    var s3ObjectId =
        s3ObjectKeyCache.computeIfAbsent(
            sha512Of(payload),
            ThrowingFunction.wrap(
                (sha512) -> {
                  var id = idGenerator.get();
                  var request =
                      PutObjectRequest.builder().bucket(bucketName).key(id.toString()).build();

                  var body = fromString(payload, UTF_8);

                  var start = OffsetDateTime.now(UTC);
                  logger.trace("uploading object to s3 with key={}", s3ArnOf(id));
                  client.putObject(request, body).get(SDK_TIMEOUT, MILLISECONDS);
                  durationHolder[0] = Duration.between(start, OffsetDateTime.now(UTC));

                  return id;
                }));

    var duration = durationHolder[0];
    logger.trace(
        "s3 offloading completed: jsonPath='{}' key={} cache={} durationMillis={}",
        jsonPathExp,
        s3ArnOf(s3ObjectId),
        duration == null ? "HIT" : "MISS",
        duration == null ? "0" : duration.toMillis());

    return s3ObjectId;
  }
}
