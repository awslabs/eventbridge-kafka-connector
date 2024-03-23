/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static com.jayway.jsonpath.Configuration.defaultConfiguration;
import static com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.panic;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.retry;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;
import static software.amazon.event.kafkaconnector.offloading.S3EventBridgeEventDetailValueOffloading.ReplaceWithDataRefJsonTransformer.replaceWithDataRef;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class S3EventBridgeEventDetailValueOffloading
    implements EventBridgeEventDetailValueOffloadingStrategy {

  private static final Logger logger =
      ContextAwareLoggerFactory.getLogger(S3EventBridgeEventDetailValueOffloading.class);

  private final String bucketName;
  private final S3Client client;
  private final ReplaceWithDataRefJsonTransformer detailValueTransformer;
  private final Supplier<UUID> idGenerator;

  public S3EventBridgeEventDetailValueOffloading(
      final S3Client client,
      final String bucketName,
      final String jsonPathExp,
      final Supplier<UUID> idGenerator) {
    this.bucketName = bucketName;
    this.client = client;
    this.detailValueTransformer = replaceWithDataRef(jsonPathExp);
    this.idGenerator = idGenerator;
  }

  @Override
  public EventBridgeEventDetailValueOffloadingResult apply(
      final List<MappedSinkRecord<PutEventsRequestEntry>> putEventsRequestEntries) {

    var result =
        putEventsRequestEntries.stream()
            .map(this::apply)
            .collect(partitioningBy(EventBridgeResult::isSuccess));

    var success = result.get(true).stream().map(EventBridgeResult::success).collect(toList());
    var errors = result.get(false).stream().map(EventBridgeResult::failure).collect(toList());

    return new EventBridgeEventDetailValueOffloadingResult(success, errors);
  }

  private EventBridgeResult<PutEventsRequestEntry> apply(
      final MappedSinkRecord<PutEventsRequestEntry> item) {

    var sinkRecord = item.getSinkRecord();
    var putEventsRequestEntry = item.getValue();

    try {

      var transformedDetail =
          detailValueTransformer.apply(
              putEventsRequestEntry.detail(),
              removedContent -> putS3(generateS3KeyOf(removedContent), removedContent));
      return success(sinkRecord, putEventsRequestEntry.copy(it -> it.detail(transformedDetail)));

    } catch (final S3Exception e) {
      // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/handling-exceptions.html
      // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
      if (e.statusCode() == 500) {
        return failure(sinkRecord, retry(e));
      }
      // TODO 429
      return failure(sinkRecord, panic(e));
    } catch (Exception e) {
      return failure(sinkRecord, panic(e));
    }
  }

  private String generateS3KeyOf(final String content) {
    return format("arn:aws:s3:::%s/%s", bucketName, idGenerator.get());
  }

  public String putS3(final String key, final String content) {
    // TODO logger
    var request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
    var response = client.putObject(request, RequestBody.fromString(content, UTF_8));
    // TODO eval response
    return key;
  }

  static class ReplaceWithDataRefJsonTransformer {

    private static final Logger logger = S3EventBridgeEventDetailValueOffloading.logger;

    private static final Configuration configuration =
        defaultConfiguration()
            .addOptions(
                SUPPRESS_EXCEPTIONS // suppress exception otherwise
                // com.jayway.jsonpath.ReadContext#read throws an exception if JSON path could not
                // be found
                );
    private static final JsonPath jsonPathAdd = JsonPath.compile("$");

    private final JsonPath jsonPathRemove;
    private final String jsonPathExp;

    private ReplaceWithDataRefJsonTransformer(final String jsonPathExp) {
      JsonPath jsonPath;
      try {
        jsonPath = JsonPath.compile(jsonPathExp);
      } catch (Exception e) {
        throw new IllegalArgumentException(format("Invalid JSON Path '%s'.", jsonPathExp));
      }

      if (!jsonPath.getPath().startsWith("$['detail']['value']")) {
        throw new IllegalArgumentException(
            format("JSON Path must start with '$.detail.value' but is '%s'.", jsonPathExp));
      }
      if (!jsonPath.isDefinite()) {
        throw new IllegalArgumentException(
            format("JSON Path must be definite but '%s' is not.", jsonPathExp));
      }

      // rewrite $.detail -> $, because PutEventsRequestEntry#detail is the sub document of $.detail
      jsonPathRemove = JsonPath.compile("$" + jsonPath.getPath().substring("$['detail']".length()));
      this.jsonPathExp = jsonPathExp;
    }

    public static ReplaceWithDataRefJsonTransformer replaceWithDataRef(String jsonPathExp) {
      return new ReplaceWithDataRefJsonTransformer(jsonPathExp);
    }

    public String apply(final String content, final Function<String, String> onDeleted) {
      var ctx = JsonPath.parse(content, configuration);
      var data = ctx.read(jsonPathRemove);
      if (data != null) {
        logger.debug("Found JSON at '{}'.", jsonPathRemove.getPath());
        String dataRefValue;
        if (data instanceof Map || data instanceof List) {
          dataRefValue = onDeleted.apply(JsonPath.parse(data).jsonString());
        } else {
          dataRefValue = onDeleted.apply(data.toString());
        }
        ctx.delete(jsonPathRemove).put(jsonPathAdd, "dataref", dataRefValue);
      }
      ctx.put(jsonPathAdd, "datarefPath", jsonPathExp);

      return ctx.jsonString();
    }
  }
}
