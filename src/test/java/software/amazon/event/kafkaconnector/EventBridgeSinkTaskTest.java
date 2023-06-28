/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static software.amazon.event.kafkaconnector.TestUtils.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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
    var firstResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 10)))
            .build();
    var secondResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(11, 15)))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(firstResponse))
        .thenReturn(completedFuture(secondResponse));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 15)));

    var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(eventBridgeAsyncClient, times(2)).putEvents(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(fromPutEventsRequestDetail(detail -> detail.get("value").get("id").asText()))
        .containsExactly(
            List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
            List.of("11", "12", "13", "14", "15"));
  }

  @Test
  @DisplayName("should resend only retryable failed records")
  public void resendRetryable() {
    var firstResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 10)))
            .build();
    var secondResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(11, 15)))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(firstResponse))
        .thenThrow(
            AwsServiceException.builder()
                .cause(EventBridgeException.builder().statusCode(500).build())
                .build())
        .thenReturn(completedFuture(secondResponse));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    task.put(OfSinkRecord.withIdsIn(rangeClosed(1, 15)));

    var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(eventBridgeAsyncClient, times(3)).putEvents(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(fromPutEventsRequestDetail(detail -> detail.get("value").get("id").asText()))
        .containsExactly(
            List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
            List.of("11", "12", "13", "14", "15"),
            List.of("11", "12", "13", "14", "15"));
  }

  @Test
  @DisplayName("should not retry size exceeding record")
  public void shouldNotRetry() {
    var secondResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(2, 10)))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenThrow(
            AwsServiceException.builder()
                .cause(EventBridgeException.builder().statusCode(413).build())
                .build())
        .thenReturn(completedFuture(secondResponse));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var task = new EventBridgeSinkTask();
    task.startInternal(config, writer, dlq);

    var records = new ArrayList<SinkRecord>();
    records.add(
        new SinkRecord(
            "topic",
            0,
            STRING_SCHEMA,
            "key",
            testSchema,
            new Struct(testSchema).put("id", "#".repeat(1024 * 256)),
            1));
    records.addAll(OfSinkRecord.withIdsIn(rangeClosed(2, 10)));

    task.put(records);

    var captor = ArgumentCaptor.forClass(PutEventsRequest.class);
    verify(eventBridgeAsyncClient, times(2)).putEvents(captor.capture());

    assertThat(captor.getAllValues())
        .extracting(fromPutEventsRequestDetail(detail -> detail.get("value").get("id").asText()))
        .containsExactly(
            List.of("#".repeat(1024 * 256)), List.of("2", "3", "4", "5", "6", "7", "8", "9", "10"));
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
