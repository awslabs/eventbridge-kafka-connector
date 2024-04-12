/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasNoJsonPath;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.isJson;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.withJsonPath;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.SchemaBuilder.array;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Index.atIndex;
import static org.assertj.core.groups.Tuple.tuple;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;
import static org.skyscreamer.jsonassert.JSONCompareMode.STRICT;
import static software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.PANIC;
import static software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.RETRY;
import static software.amazon.event.kafkaconnector.offloading.S3EventBridgeEventDetailValueOffloading.JSON_PATH_PREFIX;

import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.assertj.core.api.iterable.ThrowingExtractor;
import org.hamcrest.MatcherAssert;
import org.json.JSONException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.skyscreamer.jsonassert.JSONAssert;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;
import software.amazon.event.kafkaconnector.cache.Cache;
import software.amazon.event.kafkaconnector.cache.FifoCache;
import software.amazon.event.kafkaconnector.cache.MessageDigestCacheKey;
import software.amazon.event.kafkaconnector.mapping.DefaultEventBridgeMapper;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

@ExtendWith(MockitoExtension.class)
class S3EventBridgeEventDetailValueOffloadingTest {

  private final String BUCKET = "test";

  private static final Schema ORDER_SCHEMA =
      SchemaBuilder.struct()
          .field("orderItems", array(STRING_SCHEMA))
          .field("orderCreatedTime", OPTIONAL_STRING_SCHEMA)
          .build();

  private final Cache<MessageDigestCacheKey, UUID> s3ObjectKeyCache = (key, f) -> f.apply(key);

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
                    s3Client, BUCKET, jsonPathExp, s3ObjectKeyCache, UUID::randomUUID));
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
                    s3Client, BUCKET, jsonPathExp, s3ObjectKeyCache, UUID::randomUUID));
    assertThat(exception)
        .hasMessage(
            format("JSON Path must start with '%s' but is '%s'.", JSON_PATH_PREFIX, jsonPathExp));
  }

  @ParameterizedTest
  @ValueSource(strings = {"$.detail.value.orderItems[*]", "$.detail.value[*]"})
  public void shouldRejectNonDefiniteJsonPath(String jsonPathExp) {
    var exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new S3EventBridgeEventDetailValueOffloading(
                    s3Client, BUCKET, jsonPathExp, s3ObjectKeyCache, UUID::randomUUID));
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
                s3ObjectKeyCache,
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
            body ->
                jsonStrictEquals(
                    "{\"orderItems\":[\"item-1\",\"item-2\"],\"orderCreatedTime\":\"Wed Dec 27 18:51:39 CET 2023\"}",
                    body),
            atIndex(0));

    assertThat(actual.success)
        .hasSize(1)
        .extracting(it -> it.getValue().detail())
        .satisfies(
            detail ->
                MatcherAssert.assertThat(
                    detail,
                    isJson(
                        allOf(
                            hasNoJsonPath("$.value"),
                            hasJsonPath(
                                "$.dataref",
                                equalTo("arn:aws:s3:::test/2d10c6f6-31e9-43b4-8706-51b4cf5534d8")),
                            hasJsonPath("$.datarefJsonPath", equalTo("$.detail.value"))))),
            atIndex(0));
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
                s3ObjectKeyCache,
                () -> UUID.fromString("fd807e9d-f92c-4c45-8bbb-f8164cc75b7e"))
            .apply(mappedSinkRecords);

    assertThat(actual.success).hasSize(1);
    MatcherAssert.assertThat(
        actual.success.get(0).getValue().detail(),
        isJson(
            allOf(
                hasJsonPath("$.value", is(nullValue())),
                hasNoJsonPath("$.dataref"),
                hasNoJsonPath("$.datarefJsonPath"))));
    assertThat(actual.errors).isEmpty();
  }

  @ParameterizedTest
  @MethodSource
  public void shouldPutSubDocumentOfSinkRecordValueWithJsonValue(
      Struct value, String expectedS3ObjectPayload) {

    var future = new CompletableFuture<PutObjectResponse>();
    future.complete(null);

    when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(future);

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value.orderItems",
                s3ObjectKeyCache,
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
        .satisfies(s -> assertEquals(expectedS3ObjectPayload, s.get(0), STRICT));

    assertThat(actual.success).hasSize(1);
    MatcherAssert.assertThat(
        actual.success.get(0).getValue().detail(),
        isJson(
            allOf(
                withJsonPath(
                    "$.value",
                    allOf(
                        hasNoJsonPath("orderItems"),
                        hasJsonPath("orderCreatedTime", equalTo("Wed Dec 27 18:51:39 CET 2023")))),
                hasJsonPath(
                    "$.dataref", equalTo("arn:aws:s3:::test/d9d624dc-8452-411e-935f-edc3d62cbae2")),
                hasJsonPath("$.datarefJsonPath", equalTo("$.detail.value.orderItems")))));
    assertThat(actual.errors).isEmpty();
  }

  public static Stream<Arguments> shouldPutSubDocumentOfSinkRecordValueWithJsonValue() {
    return Stream.of(
        Arguments.of(
            new Struct(ORDER_SCHEMA)
                .put("orderItems", List.of())
                .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023"),
            "[]"),
        Arguments.of(
            new Struct(ORDER_SCHEMA)
                .put("orderItems", List.of("item-1"))
                .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023"),
            "[\"item-1\"]"),
        Arguments.of(
            new Struct(ORDER_SCHEMA)
                .put("orderItems", List.of("item-1", "item-2"))
                .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023"),
            "[\"item-1\",\"item-2\"]"));
  }

  @Test
  public void shouldPutEmptySubDocumentOfSinkRecordValueWithJsonValue() {

    var emptyStruct = SchemaBuilder.struct().build();
    var orderSchema =
        SchemaBuilder.struct()
            .field("orderItems", array(STRING_SCHEMA))
            .field("orderCreatedTime", OPTIONAL_STRING_SCHEMA)
            .field("orderHints", emptyStruct)
            .build();

    var value =
        new Struct(orderSchema)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023")
            .put("orderHints", new Struct(emptyStruct));

    var future = new CompletableFuture<PutObjectResponse>();
    future.complete(null);

    when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(future);

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", orderSchema, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value.orderHints",
                s3ObjectKeyCache,
                () -> UUID.fromString("fbdacb19-441e-479b-a347-f7c5f82ccd70"))
            .apply(mappedSinkRecords);

    verify(s3Client).putObject(putObjectRequestCaptor.capture(), requestBodyCaptor.capture());

    assertThat(putObjectRequestCaptor.getAllValues())
        .hasSize(1)
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(tuple("test", "fbdacb19-441e-479b-a347-f7c5f82ccd70"));
    assertThat(requestBodyCaptor.getAllValues())
        .hasSize(1)
        .extracting(requestBodyAsString())
        .satisfies(s -> assertEquals("{}", s.get(0), STRICT));

    assertThat(actual.success).hasSize(1);
    MatcherAssert.assertThat(
        actual.success.get(0).getValue().detail(),
        isJson(
            allOf(
                withJsonPath(
                    "$.value",
                    allOf(
                        hasJsonPath("orderItems", equalTo(List.of("item-1", "item-2"))),
                        hasJsonPath("orderCreatedTime", equalTo("Wed Dec 27 18:51:39 CET 2023")))),
                hasJsonPath(
                    "$.dataref", equalTo("arn:aws:s3:::test/fbdacb19-441e-479b-a347-f7c5f82ccd70")),
                hasJsonPath("$.datarefJsonPath", equalTo("$.detail.value.orderHints")))));
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
                s3ObjectKeyCache,
                () -> UUID.fromString("eb8421ef-3b46-4ed7-806f-7326546ed12c"))
            .apply(mappedSinkRecords);

    verifyNoInteractions(s3Client);

    assertThat(actual.success)
        .hasSize(1)
        .extracting(x -> x.getValue().detail())
        .satisfies(
            detail ->
                jsonStrictEquals(
                    "{\"topic\":\"topic\",\"partition\":0,\"offset\":0,\"timestamp\":null,\"timestampType\":\"NoTimestampType\",\"headers\":[],\"key\":\"1\",\"value\":{\"orderItems\":[\"item-1\",\"item-2\"],\"orderCreatedTime\":\"Wed Dec 27 18:51:39 CET 2023\"}}",
                    detail),
            atIndex(0));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldPutNothingOfSinkRecordValueWithSubDocumentNullJsonValue() {

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", null);

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client,
                BUCKET,
                "$.detail.value.orderCreatedTime",
                s3ObjectKeyCache,
                () -> UUID.fromString("f3dd06ed-1daf-47ff-ab3e-f273158469aa"))
            .apply(mappedSinkRecords);

    assertThat(actual.success).hasSize(1);
    MatcherAssert.assertThat(
        actual.success.get(0).getValue().detail(),
        isJson(
            allOf(
                withJsonPath(
                    "$.value",
                    allOf(
                        hasJsonPath("orderItems", equalTo(List.of("item-1", "item-2"))),
                        hasJsonPath("orderCreatedTime", is(nullValue())))),
                hasNoJsonPath("$.dataref"),
                hasNoJsonPath("$.datarefJsonPath"))));

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
                s3ObjectKeyCache,
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

    assertThat(actual.success).hasSize(1);
    MatcherAssert.assertThat(
        actual.success.get(0).getValue().detail(),
        isJson(
            allOf(
                hasNoJsonPath("$.value"),
                hasJsonPath(
                    "$.dataref", equalTo("arn:aws:s3:::test/55443a4d-4d15-49ef-a2b0-d89657a71d8a")),
                hasJsonPath("$.datarefJsonPath", equalTo("$.detail.value")))));
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
                s3ObjectKeyCache,
                () -> UUID.fromString("37c43d04-147f-4e83-9890-b41fad756377"))
            .apply(mappedSinkRecords);

    verifyNoInteractions(s3Client);

    assertThat(actual.success).hasSize(1);
    MatcherAssert.assertThat(
        actual.success.get(0).getValue().detail(),
        isJson(
            allOf(
                hasJsonPath("$.value", equalTo("Hello world")),
                hasNoJsonPath("$.dataref"),
                hasNoJsonPath("$.datarefJsonPath"))));
    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldReturnRetryError() {

    var future = new CompletableFuture<PutObjectResponse>();
    future.completeExceptionally(
        S3Exception.builder()
            .statusCode(500)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("InternalError").build())
            .build());

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
                s3Client, BUCKET, "$.detail.value", s3ObjectKeyCache, UUID::randomUUID)
            .apply(mappedSinkRecords);

    assertThat(actual.success).isEmpty();
    assertThat(actual.errors)
        .hasSize(1)
        .extracting(it -> it.getValue().getType())
        .containsExactly(RETRY);
  }

  @Test
  public void shouldReturnPanicError() {

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
                s3Client, BUCKET, "$.detail.value", s3ObjectKeyCache, UUID::randomUUID)
            .apply(mappedSinkRecords);

    assertThat(actual.success).isEmpty();
    assertThat(actual.errors)
        .hasSize(1)
        .extracting(it -> it.getValue().getType())
        .containsExactly(PANIC);
  }

  @Test
  public void shouldUseCache() {

    var future = new CompletableFuture<PutObjectResponse>();
    future.complete(null);

    when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(future);

    @SuppressWarnings("unchecked")
    var idGenerator = (Supplier<UUID>) mock(Supplier.class);
    when(idGenerator.get())
        .thenReturn(
            UUID.fromString("0c6ec17a-19e8-4dd6-8696-f91e8edc7db0"),
            UUID.fromString("322abe30-4839-4ea7-a547-c2df7be43aac"),
            UUID.fromString("905b0f71-a69f-42db-9809-44c6103c9bae"));

    var value1 =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var value2 =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-3"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value1, 0),
            new SinkRecord("topic", 0, STRING_SCHEMA, "2", ORDER_SCHEMA, value2, 1),
            new SinkRecord("topic", 0, STRING_SCHEMA, "3", ORDER_SCHEMA, value1, 2));

    var actual =
        new S3EventBridgeEventDetailValueOffloading(
                s3Client, BUCKET, "$.detail.value.orderItems", new FifoCache<>(2), idGenerator)
            .apply(mappedSinkRecords);

    verify(s3Client, times(2))
        .putObject(putObjectRequestCaptor.capture(), any(AsyncRequestBody.class));

    assertThat(putObjectRequestCaptor.getAllValues())
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(
            tuple("test", "0c6ec17a-19e8-4dd6-8696-f91e8edc7db0"),
            tuple("test", "322abe30-4839-4ea7-a547-c2df7be43aac"));

    assertThat(actual.success)
        .extracting(it -> dataref(it.getValue().detail()))
        .containsExactly(
            "arn:aws:s3:::test/0c6ec17a-19e8-4dd6-8696-f91e8edc7db0",
            "arn:aws:s3:::test/322abe30-4839-4ea7-a547-c2df7be43aac",
            "arn:aws:s3:::test/0c6ec17a-19e8-4dd6-8696-f91e8edc7db0");

    assertThat(actual.errors).isEmpty();
  }

  @Test
  public void shouldNotUpdateCacheOnException() {

    var futureException = new CompletableFuture<PutObjectResponse>();
    futureException.completeExceptionally(
        S3Exception.builder()
            .statusCode(500)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("InternalError").build())
            .build());

    var future = new CompletableFuture<PutObjectResponse>();
    future.complete(null);

    when(s3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
        .thenReturn(futureException)
        .thenReturn(future);

    @SuppressWarnings("unchecked")
    var idGenerator = (Supplier<UUID>) mock(Supplier.class);
    when(idGenerator.get())
        .thenReturn(
            UUID.fromString("0c6ec17a-19e8-4dd6-8696-f91e8edc7db0"),
            UUID.fromString("322abe30-4839-4ea7-a547-c2df7be43aac"));

    var value =
        new Struct(ORDER_SCHEMA)
            .put("orderItems", List.of("item-1", "item-2"))
            .put("orderCreatedTime", "Wed Dec 27 18:51:39 CET 2023");

    var mappedSinkRecords =
        withDefaultEventBridgeMapperMap(
            getEventBridgeSinkConfig(),
            new SinkRecord("topic", 0, STRING_SCHEMA, "1", ORDER_SCHEMA, value, 0));

    var offloading =
        new S3EventBridgeEventDetailValueOffloading(
            s3Client, BUCKET, "$.detail.value.orderItems", new FifoCache<>(2), idGenerator);

    assertThat(offloading.apply(mappedSinkRecords).errors).hasSize(1);

    var actual = offloading.apply(mappedSinkRecords);

    verify(s3Client, times(2))
        .putObject(putObjectRequestCaptor.capture(), any(AsyncRequestBody.class));

    assertThat(putObjectRequestCaptor.getAllValues())
        .extracting(PutObjectRequest::bucket, PutObjectRequest::key)
        .containsExactly(
            tuple("test", "0c6ec17a-19e8-4dd6-8696-f91e8edc7db0"),
            tuple("test", "322abe30-4839-4ea7-a547-c2df7be43aac"));

    assertThat(actual.success)
        .extracting(it -> dataref(it.getValue().detail()))
        .containsExactly("arn:aws:s3:::test/322abe30-4839-4ea7-a547-c2df7be43aac");

    assertThat(actual.errors).isEmpty();
  }

  private static String dataref(String json) {
    return JsonPath.parse(json).read(JsonPath.compile("$.dataref"));
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
      throw new IllegalStateException(
          result.errors.stream()
              .map(
                  it ->
                      String.format(
                          "'%s' Cause: %s",
                          it.getValue().getMessage(),
                          Optional.ofNullable(it.getValue().getCause())
                              .map(Throwable::toString)
                              .orElse("n/a")))
              .collect(joining()));
    }
    return result.success;
  }

  private void jsonStrictEquals(String expectedStr, String actualStr) {
    try {
      JSONAssert.assertEquals(expectedStr, actualStr, STRICT);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
}
