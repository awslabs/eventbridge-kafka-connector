/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.eventbridge.model.EventBridgeException;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;

@ExtendWith(MockitoExtension.class)
public class EventBridgeSinkTaskTest {

  private final ObjectMapper mapper = new ObjectMapper();

  @Mock private EventBridgeAsyncClient eventBridgeAsyncClient;
  @Mock private ErrantRecordReporter dlq;

  private final EventBridgeSinkConfig config =
      new EventBridgeSinkConfig(
          Map.of(
              "aws.eventbridge.retries.max", 10,
              "aws.eventbridge.connector.id", "someId",
              "aws.eventbridge.region", "eu-central-1",
              "aws.eventbridge.eventbus.arn", "something",
              "aws.eventbridge.detail.types", "my-first-${topic}"));

  @BeforeEach
  void resetMocks() {
    reset(eventBridgeAsyncClient, dlq);
  }

  @Test
  @DisplayName("should send records successfully")
  public void sendSuccessfully() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(PutEventsResponse.builder().failedEntryCount(0).build()))
        .thenReturn(completedFuture(PutEventsResponse.builder().failedEntryCount(0).build()));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    task.put(List.of(createTestRecordWithId("one"), createTestRecordWithId("two")));

    var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(eventBridgeAsyncClient, times(2)).putEvents(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(fromPutEventsRequestDetail(detail -> detail.get("value").get("id").asText()))
        .containsExactly(List.of("one"), List.of("two"));
  }

  @Test
  @DisplayName("should resend only retryable failed records")
  public void resendRetryable() {

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(PutEventsResponse.builder().failedEntryCount(0).build()))
        .thenThrow(
            AwsServiceException.builder()
                .cause(EventBridgeException.builder().statusCode(500).build())
                .build())
        .thenReturn(completedFuture(PutEventsResponse.builder().failedEntryCount(0).build()));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    task.put(List.of(createTestRecordWithId("one"), createTestRecordWithId("two")));

    var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(eventBridgeAsyncClient, times(3)).putEvents(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(fromPutEventsRequestDetail(detail -> detail.get("value").get("id").asText()))
        .containsExactly(List.of("one"), List.of("two"), List.of("two"));
  }

  private SinkRecord createTestRecordWithId(String id) {
    var valueSchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
    var value = new Struct(valueSchema).put("id", id);

    return new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", valueSchema, value, 0);
  }

  private <T> Function<PutEventsRequest, List<T>> fromPutEventsRequestDetail(
      Function<JsonNode, T> f) {
    return (PutEventsRequest request) ->
        request.entries().stream()
            .map(PutEventsRequestEntry::detail)
            .map(
                detail -> {
                  try {
                    return f.apply(mapper.readTree(detail));
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(toList());
  }
}
