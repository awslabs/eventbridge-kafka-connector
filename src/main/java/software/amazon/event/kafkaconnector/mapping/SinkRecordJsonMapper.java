/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static java.util.Collections.singletonMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

public class SinkRecordJsonMapper {
  private final JsonConverter jsonConverter = new JsonConverter();
  private final ObjectMapper objectMapper = new ObjectMapper();

  public SinkRecordJsonMapper() {
    jsonConverter.configure(singletonMap("schemas.enable", "false"), false);
  }

  public String createJsonPayload(SinkRecord sinkRecord) throws IOException {
    var root = objectMapper.createObjectNode();
    root.put("topic", sinkRecord.topic());
    root.put("partition", sinkRecord.kafkaPartition());
    root.put("offset", sinkRecord.kafkaOffset());
    root.put("timestamp", sinkRecord.timestamp());
    root.put("timestampType", sinkRecord.timestampType().toString());
    root.set("headers", createHeaderArray(sinkRecord));

    if (sinkRecord.key() == null) {
      root.set("key", null);
    } else {
      root.set(
          "key",
          createJSONFromByteArray(
              jsonConverter.fromConnectData(
                  sinkRecord.topic(), sinkRecord.keySchema(), sinkRecord.key())));
    }

    // tombstone handling
    if (sinkRecord.value() == null) {
      root.set("value", null);
    } else {
      root.set(
          "value",
          createJSONFromByteArray(
              jsonConverter.fromConnectData(
                  sinkRecord.topic(), sinkRecord.valueSchema(), sinkRecord.value())));
    }
    return root.toString();
  }

  /**
   * This method serializes Kafka message headers to JSON.
   *
   * @param sinkRecord Kafka record to be sent to EventBridge
   * @return headers to be added to EventBridge message
   * @throws IOException
   */
  private ArrayNode createHeaderArray(SinkRecord sinkRecord) throws IOException {
    var headersArray = objectMapper.createArrayNode();

    for (Header header : sinkRecord.headers()) {
      var headerItem = objectMapper.createObjectNode();
      headerItem.set(
          header.key(),
          createJSONFromByteArray(
              jsonConverter.fromConnectHeader(
                  sinkRecord.topic(), header.key(), header.schema(), header.value())));
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
