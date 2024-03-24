/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.STRICT;
import static software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.PANIC;
import static software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.RETRY;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.DelegatingS3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
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

  @Mock private S3AsyncClient s3Client;

  @Captor private ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;
  @Captor private ArgumentCaptor<AsyncRequestBody> requestBodyCaptor;

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
            () ->
                new S3EventBridgeEventDetailValueOffloading(
                    s3Client, BUCKET, jsonPathExp, UUID::randomUUID));
    assertThat(exception).hasMessage(format("Invalid JSON Path '%s'.", jsonPathExp));
  }

  @ParameterizedTest
  @ValueSource(strings = {"$", "$.detail", "$.other"})
  public void shouldRejectJsonPathWithUnsupportedPath(String jsonPathExp) {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new S3EventBridgeEventDetailValueOffloading(
                    s3Client, BUCKET, jsonPathExp, UUID::randomUUID));
    assertThat(exception)
        .hasMessage(format("JSON Path must start with '$.detail.value' but is '%s'.", jsonPathExp));
  }

  @ParameterizedTest
  @ValueSource(strings = {"$.detail.value.orderItems[*]", "$.detail.value[*]"})
  public void shouldRejectNonDefiniteJsonPath(String jsonPathExp) {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new S3EventBridgeEventDetailValueOffloading(
                    s3Client, BUCKET, jsonPathExp, UUID::randomUUID));
    assertThat(exception)
        .hasMessage(format("JSON Path must be definite but '%s' is not.", jsonPathExp));
  }

  @Test
  public void shouldPutFullSinkRecordValueWithJsonValue() {

    var future = new CompletableFuture<PutObjectResponse>();
    future.complete(null);

    when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(future);

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value",
                () -> UUID.fromString("2d10c6f6-31e9-43b4-8706-51b4cf5534d8"))
            .apply(mappedSinkRecords);

    verify(s3Client).putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    assertThat(putObjectRequestCaptor.getAllValues())
        .hasSize(1)
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(tuple("test", "2d10c6f6-31e9-43b4-8706-51b4cf5534d8"));
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
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"dataref\":\"arn:aws:s3:::test/2d10c6f6-31e9-43b4-8706-51b4cf5534d8\",\"datarefPath\":\"$.detail.value\"}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldPutNothingOfSinkRecordValueWithNullJsonValue() {

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, null, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value",
                () -> UUID.fromString("fd807e9d-f92c-4c45-8bbb-f8164cc75b7e"))
            .apply(mappedSinkRecords);

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            s ->
                assertEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"value\":null,\"datarefPath\":\"$.detail.value\"}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldPutSubDocumentOfSinkRecordValueWithJsonValue() {

    var future = new CompletableFuture<PutObjectResponse>();
    future.complete(null);

    when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(future);

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value.orderItems",
                () -> UUID.fromString("d9d624dc-8452-411e-935f-edc3d62cbae2"))
            .apply(mappedSinkRecords);

    verify(s3Client).putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    assertThat(putObjectRequestCaptor.getAllValues())
        .hasSize(1)
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(tuple("test", "d9d624dc-8452-411e-935f-edc3d62cbae2"));
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
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"value\":{\"orderCreatedTime\":\"Wed Dec 27 18:51:39 CET 2023\"},\"dataref\":\"arn:aws:s3:::test/d9d624dc-8452-411e-935f-edc3d62cbae2\",\"datarefPath\":\"$.detail.value.orderItems\"}",
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
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value.orderId",
                () -> UUID.fromString("eb8421ef-3b46-4ed7-806f-7326546ed12c"))
            .apply(mappedSinkRecords);

    verifyNoInteractions(s3Client);

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            s ->
                assertEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"value\":{\"orderItems\":[\"item-1\",\"item-2\"],\"orderCreatedTime\":\"Wed Dec 27 18:51:39 CET 2023\"},\"datarefPath\":\"$.detail.value.orderId\"}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldPutFullSinkRecordValueWithStringValue() {

    var future = new CompletableFuture<PutObjectResponse>();
    future.complete(null);

    when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(future);

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", STRING_SCHEMA, "Hello world", 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value",
                () -> UUID.fromString("55443a4d-4d15-49ef-a2b0-d89657a71d8a"))
            .apply(mappedSinkRecords);

    verify(s3Client).putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    assertThat(putObjectRequestCaptor.getAllValues())
        .hasSize(1)
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(tuple("test", "55443a4d-4d15-49ef-a2b0-d89657a71d8a"));
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
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"dataref\":\"arn:aws:s3:::test/55443a4d-4d15-49ef-a2b0-d89657a71d8a\",\"datarefPath\":\"$.detail.value\"}",
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
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value.key",
                () -> UUID.fromString("37c43d04-147f-4e83-9890-b41fad756377"))
            .apply(mappedSinkRecords);

    verifyNoInteractions(s3Client);

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            s ->
                assertEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"value\":\"Hello world\",\"datarefPath\":\"$.detail.value.key\"}",
                    s.get(0),
                    STRICT));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldReturnRetryError() {

    var s3Client =
        new DelegatingS3AsyncClient(mock(S3AsyncClient.class)) {
          @Override
          public CompletableFuture<PutObjectResponse> putObject(
              PutObjectRequest putObjectRequest, AsyncRequestBody requestBody) {
            var future = new CompletableFuture<PutObjectResponse>();

            future.completeExceptionally(
                S3Exception.builder()
                    .statusCode(500)
                    .awsErrorDetails(AwsErrorDetails.builder().errorCode("InternalError").build())
                    .build());
            return future;
          }
        };

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client, BUCKET, "$.detail.value", UUID::randomUUID)
            .apply(mappedSinkRecords);

    assertThat(actual.success).isEmpty();
    assertThat(actual.errors)
        .hasSize(1)
        .extracting(it -> it.getValue().getType())
        .containsExactly(RETRY);
  }

  @Test
  public void shouldReturnPanicError() {

    var s3Client =
        new DelegatingS3AsyncClient(mock(S3AsyncClient.class)) {
          @Override
          public CompletableFuture<PutObjectResponse> putObject(
              PutObjectRequest putObjectRequest, AsyncRequestBody requestBody) {
            var future = new CompletableFuture<PutObjectResponse>();

            future.completeExceptionally(
                S3Exception.builder()
                    .statusCode(403)
                    .awsErrorDetails(
                        AwsErrorDetails.builder()
                            .errorMessage(
                                "The AWS Access Key Id you provided does not exist in our records.")
                            .errorCode("InvalidAccessKeyId")
                            .serviceName("S3")
                            .build())
                    .build());
            return future;
          }
        };

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client, BUCKET, "$.detail.value", UUID::randomUUID)
            .apply(mappedSinkRecords);

    assertThat(actual.success).isEmpty();
    assertThat(actual.errors)
        .hasSize(1)
        .extracting(it -> it.getValue().getType())
        .containsExactly(PANIC);
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

  private static ThrowingExtractor<AsyncRequestBody, String, IOException> requestBodyAsString() {
    return (AsyncRequestBody requestBody) -> {
      final StringBuffer sb = new StringBuffer();
      try {
        requestBody
            .subscribe(bb -> sb.append(StandardCharsets.UTF_8.decode(bb)))
            .get(1000, MILLISECONDS);
      } catch (Exception ignore) {
      }
      return sb.toString();
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
