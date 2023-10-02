/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class EventBridgeSinkInterceptor<K, V> implements ConsumerInterceptor<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(EventBridgeSinkInterceptor.class);

  private final Map<String, Object> configs = new HashMap<>();
  private final ObjectMapper mapper = new ObjectMapper();

  private S3Client s3 = null;
  private String bucketName = null;
  private int thresholdBytes = 4 * 1024;

  @Override
  public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
    logger.debug("onConsume({}) {}", records, configs.get("aws.eventbridge.connector.id"));

    if ("json".equals(configs.get("aws.eventbridge.value"))) {
      final Map<TopicPartition, List<ConsumerRecord<K, V>>> result = new HashMap<>();
      for (var partition : records.partitions()) {
        var recordsOfPartition = new ArrayList<ConsumerRecord<K, V>>();
        for (var record : records.records(partition)) {
          var bytes = (byte[]) record.value();
          logger.debug("record value bytes: {}", bytes.length);
          if (bytes.length > thresholdBytes) {

            var s3key = UUID.randomUUID().toString();
            var req = PutObjectRequest.builder().bucket(bucketName).key(s3key).build();

            s3.putObject(req, RequestBody.fromBytes(bytes));

            var headers = new RecordHeaders(record.headers());
            headers.add("s3", s3key.getBytes());
            var copy =
                new ConsumerRecord<>(
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    record.timestampType(),
                    record.serializedKeySize(),
                    record.serializedValueSize(),
                    record.key(),
                    (V) null,
                    headers,
                    record.leaderEpoch());
            logger.debug("add record with reference");
            recordsOfPartition.add(copy);
          } else {
            logger.debug("add original record");
            recordsOfPartition.add(record);
          }
        }
        logger.debug("add/map {} records to partition {}", recordsOfPartition.size(), partition);
        result.put(partition, recordsOfPartition);
      }
      return new ConsumerRecords<K, V>(result);
    } else if ("string".equals(configs.get("aws.eventbridge.value"))) {
      records.forEach(
          record -> logger.info("record: {}", new String((byte[]) record.value()))); // charset?
    }

    return records;
  }

  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    logger.debug("onCommit({})", offsets);
  }

  @Override
  public void close() {
    logger.debug("close()");
    s3.close();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    logger.debug("configure({})", configs);
    this.configs.clear();
    this.configs.putAll(configs);

    var credentialProvioder = DefaultCredentialsProvider.create();
    var region = Region.of((String) configs.get("aws.eventbridge.s3.region"));
    var endpointURI = (String) configs.get("aws.eventbridge.s3.endpoint.uri");

    var s3ClientBuilder =
        S3Client.builder().region(region).credentialsProvider(credentialProvioder);

    if (endpointURI != null) {
      try {
        s3ClientBuilder.endpointOverride(new URI(endpointURI)).forcePathStyle(true);
      } catch (URISyntaxException e) {
        throw new RuntimeException(e);
      }
    }

    s3 = s3ClientBuilder.build();
    bucketName = (String) configs.get("aws.eventbridge.s3.bucket_name");
    if (configs.containsKey("aws.eventbridge.s3.threshold_bytes")) {
      thresholdBytes = Integer.parseInt((String) configs.get("aws.eventbridge.s3.threshold_bytes"));
    }
  }
}
