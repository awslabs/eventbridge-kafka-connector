/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.panic;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.retry;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;
import static software.amazon.event.kafkaconnector.offloading.ReplaceWithDataRefJsonTransformer.replaceWithDataRef;

import java.util.List;
import java.util.UUID;
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

  public DataRef putS3(final String key, final String content) {
    // TODO logger
    var request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
    var response = client.putObject(request, RequestBody.fromString(content, UTF_8));
    // TODO eval response
    return DataRef.of(key);
  }
}
