/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static java.lang.String.format;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.STRICT;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.core.api.iterable.ThrowingExtractor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;
import software.amazon.event.kafkaconnector.mapping.DefaultEventBridgeMapper;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

@ExtendWith(MockitoExtension.class)
class S3EventBridgeEventDetailValueOffloadingTest {

  private final String BUCKET = "test";

  private static final Schema ORDER_SCHEMA =
      SchemaBuilder.struct()
          .field("orderItems", SchemaBuilder.array(STRING_SCHEMA))
          .field("orderCreatedTime", STRING_SCHEMA)
          .build();

  @Mock private S3Client s3Client;

  @Captor private ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;
  @Captor private ArgumentCaptor<RequestBody> requestBodyCaptor;

  @BeforeEach
  void resetMocks() {
    reset(s3Client);
  }

  @AfterEach
  void verifyNoMoreMocksInteractions() {
    verifyNoMoreInteractions(s3Client);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "$.detail..", "$.detail[value]"})
  public void shouldRejectInvalidJsonPath(String jsonPathExp) {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new S3EventBridgeEventDetailValueOffloading(s3Client, BUCKET, jsonPathExp));
    assertThat(exception).hasMessage(format("Invalid JSON Path '%s'.", jsonPathExp));
  }

  @ParameterizedTest
  @ValueSource(strings = {"$", "$.detail", "$.other"})
  public void shouldRejectJsonPathWithUnsupportedPath(String jsonPathExp) {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new S3EventBridgeEventDetailValueOffloading(s3Client, BUCKET, jsonPathExp));
    assertThat(exception)
        .hasMessage(format("JSON Path must start with '$.detail.value' but is '%s'.", jsonPathExp));
  }

  @ParameterizedTest
  @ValueSource(strings = {"$.detail.value.orderItems[*]", "$.detail.value[*]"})
  public void shouldRejectNonDefiniteJsonPath(String jsonPathExp) {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> new S3EventBridgeEventDetailValueOffloading(s3Client, BUCKET, jsonPathExp));
    assertThat(exception)
        .hasMessage(format("JSON Path must be definite but '%s' is not.", jsonPathExp));
  }

  @Test
  public void shouldPutFullSinkRecordValueWithJsonValue() {

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(s3Client, BUCKET, "$.detail.value")
            .apply(mappedSinkRecords);

    verify(s3Client).putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    assertThat(putObjectRequestCaptor.getAllValues())
        .hasSize(1)
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(
            tuple(
                "test",
                "arn:aws:s3:::test/c36ddc438c6150350897ef33165a7a524e8138a9a6f357302e541f1fcbff1f9f"));
    assertThat(requestBodyCaptor.getAllValues())
        .hasSize(1)
        .extracting(requestBodyAsString())
        .satisfies(
            s ->
                assertEquals(
                    "{\"orderItems\":[\"item-1\",\"item-2\"],\"orderCreatedTime\":\"Wed Dec 27 18:51:39 CET 2023\"}",
                    s.get(0),
                    STRICT));

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            s ->
                assertEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"dataref\":\"arn:aws:s3:::test/c36ddc438c6150350897ef33165a7a524e8138a9a6f357302e541f1fcbff1f9f\"}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldPutSubDocumentOfSinkRecordValueWithJsonValue() {

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(s3Client, BUCKET, "$.detail.value.orderItems")
            .apply(mappedSinkRecords);

    verify(s3Client).putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    assertThat(putObjectRequestCaptor.getAllValues())
        .hasSize(1)
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(
            tuple(
                "test",
                "arn:aws:s3:::test/c36ddc438c6150350897ef33165a7a524e8138a9a6f357302e541f1fcbff1f9f"));
    assertThat(requestBodyCaptor.getAllValues())
        .hasSize(1)
        .extracting(requestBodyAsString())
        .satisfies(s -> assertEquals("[\"item-1\",\"item-2\"]", s.get(0), STRICT));

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            s ->
                assertEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"value\":{\"orderCreatedTime\":\"Wed Dec 27 18:51:39 CET 2023\"},\"dataref\":\"arn:aws:s3:::test/c36ddc438c6150350897ef33165a7a524e8138a9a6f357302e541f1fcbff1f9f\"}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldPutNothingOfSinkRecordValueWithJsonValue() {

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(s3Client, BUCKET, "$.detail.value.orderId")
            .apply(mappedSinkRecords);

    verifyNoInteractions(s3Client);

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            s ->
                assertEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"value\":{\"orderItems\":[\"item-1\",\"item-2\"],\"orderCreatedTime\":\"Wed Dec 27 18:51:39 CET 2023\"}}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldPutFullSinkRecordValueWithStringValue() {

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", STRING_SCHEMA, "Hello world", 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(s3Client, BUCKET, "$.detail.value")
            .apply(mappedSinkRecords);

    verify(s3Client).putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    assertThat(putObjectRequestCaptor.getAllValues())
        .hasSize(1)
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(
            tuple(
                "test",
                "arn:aws:s3:::test/b58f34e73579dcfb700daacadd50fa7503f1e4c6c881cb4d720fe84a57be306d"));
    assertThat(requestBodyCaptor.getAllValues())
        .hasSize(1)
        .extracting(requestBodyAsString())
        .containsExactly("Hello world");

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            s ->
                assertEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"dataref\":\"arn:aws:s3:::test/b58f34e73579dcfb700daacadd50fa7503f1e4c6c881cb4d720fe84a57be306d\"}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldPutNothingOfSinkRecordValueWithStringValue() {

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", STRING_SCHEMA, "Hello world", 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(s3Client, BUCKET, "$.detail.value.key")
            .apply(mappedSinkRecords);

    verifyNoInteractions(s3Client);

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            s ->
                assertEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"value\":\"Hello world\"}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  private static EventBridgeSinkConfig getEventBridgeSinkConfig() {
    var properties = new Properties();
    properties.setProperty("aws.eventbridge.connector.id", "eventbridge-e2e-connector");
    properties.setProperty("aws.eventbridge.region", "us-east-1");
    properties.setProperty("aws.eventbridge.endpoint.uri", "http://localstack:4566");
    properties.setProperty(
        "aws.eventbridge.eventbus.arn",
        "arn:aws:events:us-east-1:000000000000:event-bus/eventbridge-e2e");

    return new EventBridgeSinkConfig(properties);
  }

  private static ThrowingExtractor<RequestBody, String, IOException> requestBodyAsString() {
    return (RequestBody requestBody) -> {
      try (var stream = requestBody.contentStreamProvider().newStream()) {
        return new String(stream.readAllBytes());
      }
    };
  }

  private List<MappedSinkRecord<PutEventsRequestEntry>> withDefaultEventBridgeMapperMap(
      final EventBridgeSinkConfig config, final SinkRecord... values) {
    var mapper = new DefaultEventBridgeMapper(config);
    var result = mapper.map(List.of(values));
    if (!result.errors.isEmpty()) {
      throw new IllegalStateException("TODO");
    }
    return result.success;
  }
}
