/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.event.kafkaconnector.mapping.TestDetailTypeMapper.DETAIL_TYPE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class DefaultEventBridgeMapperTest {

  @Test
  @DisplayName("Mapping without timeMapperClass parameter defined should give time null")
  public void mapWithDefaultTimeMapper() {
    var config = new EventBridgeSinkConfig(defaultConfig());
    var mapper = new DefaultEventBridgeMapper(config);
    var result = mapper.map(List.of(defaultSinkRecord()));
    var putEventsRequestEntry = result.success.stream().findFirst().get();
    assertThat(putEventsRequestEntry.getValue().time()).isNull();
  }

  @Test
  @DisplayName("Mapping with timeMapperClass parameter defined should give a time value")
  public void mapWithSpecifiedTimeMapper() {
    var props = new HashMap<>(defaultConfig());
    props.put(
        "aws.eventbridge.time.mapper.class",
        "software.amazon.event.kafkaconnector.mapping.TestTimeMapper");
    var config = new EventBridgeSinkConfig(props);
    var mapper = new DefaultEventBridgeMapper(config);
    var result = mapper.map(List.of(defaultSinkRecord()));
    var putEventsRequestEntry = result.success.stream().findFirst().get();
    assertThat(putEventsRequestEntry.getValue().time()).isEqualTo("1981-12-24T00:00:00Z");
  }

  @Test
  @DisplayName(
      "Mapping with DetailTypeMapperClass parameter defined should give custom detail type value")
  public void mapWithSpecifiedDetailTypeMapper() {
    var props = new HashMap<>(defaultConfig());
    props.put(
        "aws.eventbridge.detail.types.mapper.class",
        "software.amazon.event.kafkaconnector.mapping.TestDetailTypeMapper");
    var config = new EventBridgeSinkConfig(props);
    var mapper = new DefaultEventBridgeMapper(config);
    var result = mapper.map(List.of(defaultSinkRecord()));
    var putEventsRequestEntry = result.success.stream().findFirst().get();
    assertThat(putEventsRequestEntry.getValue().detailType()).isEqualTo(DETAIL_TYPE);
  }

  @NotNull
  private static SinkRecord defaultSinkRecord() {
    return new SinkRecord(
        "topic-1",
        0,
        null,
        "key-1",
        null,
        Map.of("timeField", "1981-12-24T00:00:00Z"),
        0L,
        1234568790123L,
        TimestampType.CREATE_TIME);
  }

  @NotNull
  private static Map<Object, Object> defaultConfig() {
    return Map.of(
        "aws.eventbridge.retries.max", 10,
        "aws.eventbridge.connector.id", "testConnectorId",
        "aws.eventbridge.region", "us-east-1",
        "aws.eventbridge.eventbus.arn", "arn:aws:events:us-east-1:000000000000:event-bus/e2e",
        "aws.eventbridge.detail.types", "test-${topic}");
  }
}
