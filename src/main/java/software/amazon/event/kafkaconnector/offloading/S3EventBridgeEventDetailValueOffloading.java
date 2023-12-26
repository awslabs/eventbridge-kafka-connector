/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static com.jayway.jsonpath.Configuration.defaultConfiguration;
import static com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.reportOnly;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;
import static software.amazon.event.kafkaconnector.offloading.S3EventBridgeEventDetailValueOffloading.ReplaceWithDataRefJsonTransformer.replaceWithDataRef;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class S3EventBridgeEventDetailValueOffloading
    implements EventBridgeEventDetailValueOffloadingStrategy {

  private static final Logger logger =
      ContextAwareLoggerFactory.getLogger(S3EventBridgeEventDetailValueOffloading.class);

  private final S3Client client;
  private final ReplaceWithDataRefJsonTransformer detailValueTransformer;

  public S3EventBridgeEventDetailValueOffloading(final S3Client client, final String jsonPath) {
    this.detailValueTransformer = replaceWithDataRef(jsonPath);
    this.client = client;
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
      var s3key = "arn:aws:s3:::bucket_name/key_name"; // TODO S3 Key generator
      var putEventsRequestEntry =
          mappedSinkRecord
              .getValue()
              .copy(
                  it ->
                      it.detail(
                          detailValueTransformer.apply(
                              mappedSinkRecord.getValue().detail(),
                              removedContent -> put(removedContent, s3key),
                              s3key)));

      return success(mappedSinkRecord.getSinkRecord(), putEventsRequestEntry);
    } catch (Exception e) {
      return failure(mappedSinkRecord.getSinkRecord(), reportOnly("TODO", e));
    }
  }

  public void put(final String content, final String s3key) {
    var request = PutObjectRequest.builder().bucket("bucket_name" /* TODO */).key(s3key).build();
    var response = client.putObject(request, RequestBody.fromString(content, UTF_8));
    // TODO eval response
  }

  static class ReplaceWithDataRefJsonTransformer {

    private static final Logger logger =
        LoggerFactory.getLogger(ReplaceWithDataRefJsonTransformer.class);

    private static final Configuration configuration =
        defaultConfiguration().addOptions(SUPPRESS_EXCEPTIONS);
    private static final JsonPath jsonPathAdd = JsonPath.compile("$.detail");
    private static final String dataRefKey = "dataref";

    private final JsonPath jsonPathRemove;

    private ReplaceWithDataRefJsonTransformer(String jsonPath) {
      jsonPathRemove = JsonPath.compile(jsonPath);
      // TODO check
      //  jsonPathRemove.getPath().startsWith("$['detail']")
      //  jsonPathRemove.isDefinite()
    }

    public static ReplaceWithDataRefJsonTransformer replaceWithDataRef(String jsonPath) {
      return new ReplaceWithDataRefJsonTransformer(jsonPath);
    }

    public String apply(String content, Consumer<String> onDeleted, String dataRefValue) {
      var ctx = JsonPath.parse(content, configuration);
      var data = ctx.read(jsonPathRemove);
      if (data != null) {
        logger.info("Found JSON at '{}'.", jsonPathRemove.getPath());
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
