/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static software.amazon.event.kafkaconnector.TestUtils.*;

import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.eventbridge.model.EventBridgeException;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

@ExtendWith(MockitoExtension.class)
public class EventBridgeWriterTest {

  private final EventBridgeSinkConfig config =
      new EventBridgeSinkConfig(
          Map.of(
              "aws.eventbridge.retries.max", 10,
              "aws.eventbridge.connector.id", "testConnectorId",
              "aws.eventbridge.region", "us-east-1",
              "aws.eventbridge.eventbus.arn", "arn:aws:events:us-east-1:000000000000:event-bus/e2e",
              "aws.eventbridge.detail.types", "test-${topic}"));

  private final EventBridgeSinkConfig configGlobalEndpoints =
      new EventBridgeSinkConfig(
          Map.of(
              "aws.eventbridge.connector.id", "testConnectorId",
              "aws.eventbridge.region", "us-east-1",
              "aws.eventbridge.eventbus.arn", "arn:aws:events:us-east-1:000000000000:event-bus/e2e",
              "aws.eventbridge.eventbus.global.endpoint.id", "abcd.xyz",
              "aws.eventbridge.detail.types", "test-${topic}"));

  @Mock private EventBridgeAsyncClient eventBridgeAsyncClient;
  @Captor private ArgumentCaptor<PutEventsRequest> putEventsArgumentCaptor;

  @BeforeEach
  public void resetMocks() {
    reset(eventBridgeAsyncClient);
  }

  @Test
  public void returnsNoFailedRecords() {
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
    var result = writer.putItems(OfSinkRecord.withIdsIn(rangeClosed(1, 15)));

    assertThat(result).filteredOn(EventBridgeResult::isSuccess).hasSize(15);
    assertThat(result).filteredOn(EventBridgeResult::isFailure).isEmpty();

    verify(eventBridgeAsyncClient, times(2)).putEvents(putEventsArgumentCaptor.capture());
    verifyNoMoreInteractions(eventBridgeAsyncClient);
    assertThat(putEventsArgumentCaptor.getAllValues()).hasSize(2);
  }

  @Test
  public void returnsFailureWithRecoverableErrorOnEventBridgeError() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenThrow(EventBridgeException.class);

    var myWriter = new EventBridgeWriter(eventBridgeAsyncClient, config);

    var result = myWriter.putItems(OfSinkRecord.withIdsIn(rangeClosed(1, 10)));

    assertThat(result)
        .filteredOn(EventBridgeResult::isFailure)
        .map(EventBridgeResult::failure)
        .extracting(it -> it.getValue().getType())
        .containsExactlyElementsOf(
            Stream.generate(() -> EventBridgeResult.ErrorType.RETRY).limit(10).collect(toList()));

    verify(eventBridgeAsyncClient).putEvents(any(PutEventsRequest.class));
    verifyNoMoreInteractions(eventBridgeAsyncClient);
  }

  @Test
  public void populatesPartialErrors() {
    var firstResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 10)))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(firstResponse))
        .thenThrow(EventBridgeException.builder().message("Failed").build());

    var myWriter = new EventBridgeWriter(eventBridgeAsyncClient, config);
    var result = myWriter.putItems(OfSinkRecord.withIdsIn(rangeClosed(1, 15)));

    assertThat(result)
        .filteredOn(EventBridgeResult::isSuccess)
        .map(EventBridgeResult::success)
        .hasSize(10);
    assertThat(result)
        .filteredOn(EventBridgeResult::isFailure)
        .map(EventBridgeResult::failure)
        .extracting(it -> it.getValue().getType())
        .containsExactlyElementsOf(
            Stream.generate(() -> EventBridgeResult.ErrorType.RETRY).limit(5).collect(toList()));

    verify(eventBridgeAsyncClient, times(2)).putEvents(putEventsArgumentCaptor.capture());
    verifyNoMoreInteractions(eventBridgeAsyncClient);
    assertThat(putEventsArgumentCaptor.getAllValues()).hasSize(2);
  }

  @Test
  public void populatesPartialDataException() {
    var firstResponse =
        PutEventsResponse.builder()
            .failedEntryCount(1)
            .entries(
                Stream.concat(
                        OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 9)).stream(),
                        Stream.of(
                            PutEventsResultEntry.builder()
                                .errorCode("errorCode")
                                .errorMessage("errorMessage")
                                .build()))
                    .collect(toList()))
            .build();
    var secondResponse =
        PutEventsResponse.builder()
            .failedEntryCount(1)
            .entries(
                Stream.concat(
                        Stream.of(
                            PutEventsResultEntry.builder()
                                .errorCode("errorCode")
                                .errorMessage("errorMessage")
                                .build()),
                        OfPutEventsResultEntry.withIdsIn(rangeClosed(12, 15)).stream())
                    .collect(toList()))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(firstResponse))
        .thenReturn(completedFuture(secondResponse));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, config);
    var result = writer.putItems(OfSinkRecord.withIdsIn(rangeClosed(1, 15)));

    assertThat(result)
        .filteredOn(EventBridgeResult::isFailure)
        .map(EventBridgeResult::failure)
        .extracting(
            it -> String.format("%s|%d", it.getValue().getType(), it.getSinkRecord().kafkaOffset()))
        .containsExactly("REPORT_ONLY|10", "REPORT_ONLY|11");

    verify(eventBridgeAsyncClient, times(2)).putEvents(putEventsArgumentCaptor.capture());
    verifyNoMoreInteractions(eventBridgeAsyncClient);
    assertThat(putEventsArgumentCaptor.getAllValues()).hasSize(2);
  }

  @Test
  public void usesGlobalEndpointId() {
    var firstResponse =
        PutEventsResponse.builder()
            .failedEntryCount(0)
            .entries(OfPutEventsResultEntry.withIdsIn(rangeClosed(1, 5)))
            .build();

    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(completedFuture(firstResponse));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, configGlobalEndpoints);
    var result = writer.putItems(OfSinkRecord.withIdsIn(rangeClosed(1, 5)));

    assertThat(result)
        .filteredOn(EventBridgeResult::isSuccess)
        .map(EventBridgeResult::success)
        .hasSize(5);

    verify(eventBridgeAsyncClient, times(1)).putEvents(putEventsArgumentCaptor.capture());
    verifyNoMoreInteractions(eventBridgeAsyncClient);
    assertThat(putEventsArgumentCaptor.getValue().endpointId()).isEqualTo("abcd.xyz");
  }
}
