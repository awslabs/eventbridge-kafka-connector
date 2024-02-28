/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static com.jayway.jsonpath.Configuration.defaultConfiguration;
import static com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static software.amazon.awssdk.utils.BinaryUtils.toHex;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.panic;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.retry;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;
import static software.amazon.event.kafkaconnector.offloading.S3EventBridgeEventDetailValueOffloading.ReplaceWithDataRefJsonTransformer.replaceWithDataRef;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private final JsonConverter jsonConverter;

  public S3EventBridgeEventDetailValueOffloading(
      final S3Client client, final String bucketName, final String jsonPathExp) {
    this.bucketName = bucketName;
    this.client = client;
    this.detailValueTransformer = replaceWithDataRef(jsonPathExp);
    this.jsonConverter = new JsonConverter();
    this.jsonConverter.configure(singletonMap("schemas.enable", "false"), false);
  }

  @Override
  public EventBridgeEventDetailValueOffloadingResult apply(
      List<MappedSinkRecord<PutEventsRequestEntry>> putEventsRequestEntries) {

    final Map<Boolean, List<EventBridgeResult<PutEventsRequestEntry>>> result =
        putEventsRequestEntries.stream()
            .map(this::apply)
            .collect(partitioningBy(EventBridgeResult::isSuccess));

    var success = result.get(true).stream().map(EventBridgeResult::success).collect(toList());
    var errors = result.get(false).stream().map(EventBridgeResult::failure).collect(toList());

    return new EventBridgeEventDetailValueOffloadingResult(success, errors);
  }

  private EventBridgeResult<PutEventsRequestEntry> apply(
      MappedSinkRecord<PutEventsRequestEntry> mappedSinkRecord) {
    try {

      var transformedDetail = extractJsonPathFromDetailAndPutS3Object(mappedSinkRecord);
      var putEventsRequestEntry =
          mappedSinkRecord.getValue().copy(it -> it.detail(transformedDetail));

      return success(mappedSinkRecord.getSinkRecord(), putEventsRequestEntry);

    } catch (S3Exception e) {
      // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/handling-exceptions.html
      // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
      if (e.statusCode() == 500) {
        return failure(mappedSinkRecord.getSinkRecord(), retry(e));
      }
      // TODO 429
      return failure(mappedSinkRecord.getSinkRecord(), panic(e));
    } catch (Exception e) {
      return failure(mappedSinkRecord.getSinkRecord(), panic(e));
    }
  }

  private String extractJsonPathFromDetailAndPutS3Object(
      MappedSinkRecord<PutEventsRequestEntry> mappedSinkRecord) throws NoSuchAlgorithmException {
    var s3key = generateS3Key(mappedSinkRecord.getSinkRecord());
    return detailValueTransformer.apply(
        mappedSinkRecord.getValue().detail(), removedContent -> put(removedContent, s3key), s3key);
  }

  private String generateS3Key(final SinkRecord sinkRecord) throws NoSuchAlgorithmException {
    var payload = payloadOf(sinkRecord);
    var s3Key = format("arn:aws:s3:::%s/%s", bucketName, toHex(sha256Of(payload)));

    logger.debug("Generated S3 key={} for payload={}.", s3Key, new String(payload));
    return s3Key;
  }

  private byte[] payloadOf(SinkRecord sinkRecord) {
    return jsonConverter.fromConnectData(
        sinkRecord.topic(), sinkRecord.valueSchema(), sinkRecord.value());
  }

  private static byte[] sha256Of(byte[] payload) throws NoSuchAlgorithmException {
    // MessageDigest is not thread safe
    return MessageDigest.getInstance("SHA-256").digest(payload);
  }

  public void put(final String content, final String s3key) {
    var request = PutObjectRequest.builder().bucket(bucketName).key(s3key).build();
    var response = client.putObject(request, RequestBody.fromString(content, UTF_8));
    // TODO eval response
  }

  static class ReplaceWithDataRefJsonTransformer {

    private static final Logger logger =
        LoggerFactory.getLogger(ReplaceWithDataRefJsonTransformer.class);

    private static final Configuration configuration =
        defaultConfiguration()
            .addOptions(
                SUPPRESS_EXCEPTIONS // suppress exception otherwise
                // com.jayway.jsonpath.ReadContext#read throws an exception if JSON path could not
                // be found
                );
    private static final JsonPath jsonPathAdd = JsonPath.compile("$");
    private static final String dataRefKey = "dataref";

    private final JsonPath jsonPathRemove;

    private ReplaceWithDataRefJsonTransformer(String jsonPathExp) {
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
    }

    public static ReplaceWithDataRefJsonTransformer replaceWithDataRef(String jsonPathExp) {
      return new ReplaceWithDataRefJsonTransformer(jsonPathExp);
    }

    public String apply(String content, Consumer<String> onDeleted, String dataRefValue) {
      var ctx = JsonPath.parse(content, configuration);
      var data = ctx.read(jsonPathRemove);
      if (data != null) {
        logger.debug("Found JSON at '{}'.", jsonPathRemove.getPath());
        if (data instanceof Map || data instanceof List) {
          onDeleted.accept(JsonPath.parse(data).jsonString());
        } else {
          onDeleted.accept(data.toString());
        }
        ctx.delete(jsonPathRemove).put(jsonPathAdd, dataRefKey, dataRefValue);
      }

      return ctx.jsonString();
    }
  }
}
