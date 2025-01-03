/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.AWS_DETAIL_TYPES_MAPPER_JSON_PATH_MAPPER_FIELDREF;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JsonPathDetailTypeMapperTest {
  private static final String TEST_TOPIC = "test-topic";
  private static final String TEST_PATH = "$.event-type";
  private DetailTypeMapper mapper;

  @BeforeEach
  void resetMapper() {
    this.mapper = new JsonPathDetailTypeMapper();
  }

  private SinkRecord createSinkRecord(String topic, Map<String, Object> value) {
    return new SinkRecord(
        topic, 0, null, "key-1", null, value, 0L, 1234568790123L, TimestampType.CREATE_TIME);
  }

  private void configureMapperWithPath(String jsonPath) {
    var config = new HashMap<>(defaultConfig());
    if (jsonPath != null) {
      config.put(AWS_DETAIL_TYPES_MAPPER_JSON_PATH_MAPPER_FIELDREF, jsonPath);
    }
    mapper.configure(new EventBridgeSinkConfig(config));
  }

  @Test
  @DisplayName("Exception when record has no topic")
  void exceptionWhenRecordHasNoTopic() {
    configureMapperWithPath(TEST_PATH);
    var invalidRecord = createSinkRecord("", Map.of("somekey", "somevalue"));

    var exception =
        assertThrows(IllegalArgumentException.class, () -> mapper.getDetailType(invalidRecord));
    assertThat(exception.getMessage()).contains("SinkRecord topic is null or empty");
  }

  @ParameterizedTest
  @DisplayName("Uses topic as fallback when JSON path value is invalid")
  @MethodSource("provideFallbackTestCases")
  void returnsTopicAsFallback(Map<String, Object> value) {
    configureMapperWithPath(TEST_PATH);

    var record = createSinkRecord(TEST_TOPIC, value);
    assertThat(mapper.getDetailType(record)).isEqualTo(TEST_TOPIC);
  }

  private static Stream<Map<String, Object>> provideFallbackTestCases() {
    return Stream.of(
        Map.of("some-key", "some-value"), // path not found
        Map.of("event-type", Map.of("key2", "val2")), // not a string
        Map.of("event-type", "") // empty string
        );
  }

  @Test
  @DisplayName("Uses value from JSON path as detail type")
  void returnsJsonPathValue() {
    configureMapperWithPath(TEST_PATH);

    var record = createSinkRecord(TEST_TOPIC, Map.of("event-type", "test.event.v0"));
    assertThat(mapper.getDetailType(record)).isEqualTo("test.event.v0");
  }

  @Test
  @DisplayName("Exception when providing empty JSON path")
  void configExceptionWhenProvidingEmptyJsonPath() {
    var exception =
        assertThrows(IllegalArgumentException.class, () -> configureMapperWithPath(null));
    assertThat(exception.getMessage()).contains("JSON path configuration must be provided");
  }

  @Test
  @DisplayName("Exception when providing non-definite JSON path")
  void configExceptionWhenProvidingNonDefiniteJsonPath() {
    var exception =
        assertThrows(
            IllegalArgumentException.class, () -> configureMapperWithPath("$..somenestedkey"));

    assertThat(exception.getMessage()).contains("JSON path must be definite");
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
