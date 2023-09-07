/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.reportOnly;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.List;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class DefaultEventBridgeMapper implements EventBridgeMapper {

  private static final String sourcePrefix = "kafka-connect.";

  private final EventBridgeSinkConfig config;
  private final JsonConverter jsonConverter = new JsonConverter();
  private final ObjectMapper objectMapper = new ObjectMapper();

  public DefaultEventBridgeMapper(EventBridgeSinkConfig config) {
    jsonConverter.configure(singletonMap("schemas.enable", "false"), false);
    this.config = config;
  }

  public EventBridgeMappingResult map(List<SinkRecord> records) {
    var partition =
        records.stream()
            .map(this::createPutEventsEntry)
            .collect(partitioningBy(EventBridgeResult::isSuccess));

    var successfulMappedRecords =
        partition.get(true).stream().map(EventBridgeResult::success).collect(toList());
    var failedMappedRecords =
        partition.get(false).stream().map(EventBridgeResult::failure).collect(toList());

    return new EventBridgeMappingResult(successfulMappedRecords, failedMappedRecords);
  }

  private EventBridgeResult<PutEventsRequestEntry> createPutEventsEntry(SinkRecord record) {
    try {
      return success(
          record,
          PutEventsRequestEntry.builder()
              .eventBusName(config.eventBusArn)
              .source(sourcePrefix + config.connectorId)
              .detailType(config.getDetailType(record.topic()))
              .resources(config.resources)
              .detail(createJsonPayload(record))
              .build());
    } catch (Exception e) {
      return failure(record, reportOnly("Cannot convert Kafka record to EventBridge.", e));
    }
  }

  private String createJsonPayload(SinkRecord record) throws IOException {
    var root = objectMapper.createObjectNode();
    root.put("topic", record.topic());
    root.put("partition", record.kafkaPartition());
    root.put("offset", record.kafkaOffset());
    root.put("timestamp", record.timestamp());
    root.put("timestampType", record.timestampType().toString());
    root.set("headers", createHeaderArray(record));

    if (record.key() == null) {
      root.set("key", null);
    } else {
      root.set(
          "key",
          createJSONFromByteArray(
              jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key())));
    }

    // tombstone handling
    if (record.value() == null) {
      root.set("value", null);
    } else {
      root.set(
          "value",
          createJSONFromByteArray(
              jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value())));
    }

    return root.toString();
  }

  /**
   * This method serializes Kafka message headers to JSON.
   *
   * @param record Kafka record to be sent to EventBridge
   * @return headers to be added to EventBridge message
   * @throws IOException
   */
  private ArrayNode createHeaderArray(SinkRecord record) throws IOException {
    var headersArray = objectMapper.createArrayNode();

    for (Header header : record.headers()) {
      var headerItem = objectMapper.createObjectNode();
      headerItem.set(
          header.key(),
          createJSONFromByteArray(
              jsonConverter.fromConnectHeader(
                  record.topic(), header.key(), header.schema(), header.value())));
      headersArray.add(headerItem);
    }
    return headersArray;
  }

  /**
   * This method converts the byteArray which is returned by the {@link JsonConverter} to JSON.
   *
   * @param jsonBytes - byteArray to convert to JSON
   * @return the JSON representation of jsonBytes
   * @throws IOException
   */
  private JsonNode createJSONFromByteArray(byte[] jsonBytes) throws IOException {
    return objectMapper.readTree(jsonBytes);
  }
}
