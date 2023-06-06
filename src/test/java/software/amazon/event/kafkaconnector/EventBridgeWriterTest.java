/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
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

  @Mock private EventBridgeAsyncClient eventBridgeAsyncClient;
  @Captor private ArgumentCaptor<PutEventsRequest> putEventsArgumentCaptor;

  @BeforeEach
  public void resetMocks() {
    reset(eventBridgeAsyncClient);
  }

  @Test
  public void returnsNoFailedRecords() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(PutEventsResultEntry.builder().eventId("eventId:1").build())
                    .build()))
        .thenReturn(
            completedFuture(
                PutEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(PutEventsResultEntry.builder().eventId("eventId:2").build())
                    .build()));

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, mock(EventBridgeSinkConfig.class));
    var result = writer.putItems(List.of(createTestRecord(), createTestRecord()));

    assertThat(result).filteredOn(EventBridgeResult::isSuccess).hasSize(2);
    assertThat(result).filteredOn(EventBridgeResult::isFailure).isEmpty();

    verify(eventBridgeAsyncClient, times(2)).putEvents(putEventsArgumentCaptor.capture());
    verifyNoMoreInteractions(eventBridgeAsyncClient);
    assertThat(putEventsArgumentCaptor.getAllValues()).hasSize(2);
  }

  @Test
  public void returnsFailureWithRecoverableErrorOnEventBridgeError() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenThrow(EventBridgeException.class);

    var myWriter = new EventBridgeWriter(eventBridgeAsyncClient, mock(EventBridgeSinkConfig.class));

    var result = myWriter.putItems(List.of(createTestRecord()));

    assertThat(result)
        .hasSize(1)
        .filteredOn(EventBridgeResult::isFailure)
        .map(EventBridgeResult::failure)
        .extracting(it -> it.getValue().getType())
        .containsExactly(EventBridgeResult.ErrorType.RETRY);

    verify(eventBridgeAsyncClient).putEvents(any(PutEventsRequest.class));
    verifyNoMoreInteractions(eventBridgeAsyncClient);
  }

  @Test
  public void populatesPartialErrors() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(PutEventsResultEntry.builder().eventId("eventId:1").build())
                    .build()))
        .thenThrow(EventBridgeException.builder().message("Failed").build());

    var myWriter = new EventBridgeWriter(eventBridgeAsyncClient, mock(EventBridgeSinkConfig.class));
    var result = myWriter.putItems(List.of(createTestRecord(), createTestRecord()));

    assertThat(result)
        .filteredOn(EventBridgeResult::isSuccess)
        .map(EventBridgeResult::success)
        .hasSize(1);
    assertThat(result)
        .filteredOn(EventBridgeResult::isFailure)
        .map(EventBridgeResult::failure)
        .hasSize(1)
        .extracting(it -> it.getValue().getType())
        .containsExactly(EventBridgeResult.ErrorType.RETRY);

    verify(eventBridgeAsyncClient, times(2)).putEvents(putEventsArgumentCaptor.capture());
    verifyNoMoreInteractions(eventBridgeAsyncClient);
    assertThat(putEventsArgumentCaptor.getAllValues()).hasSize(2);
  }

  @Test
  public void populatesPartialDataException() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(
            completedFuture(
                PutEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(PutEventsResultEntry.builder().eventId("eventId:1").build())
                    .build()))
        .thenReturn(
            completedFuture(
                PutEventsResponse.builder()
                    .failedEntryCount(0)
                    .entries(PutEventsResultEntry.builder().eventId("eventId:2").build())
                    .build()));

    var record = createTestRecord();
    SinkRecord tombstone =
        new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "somekey", Schema.STRING_SCHEMA, null, 0);

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, mock(EventBridgeSinkConfig.class));
    var result = writer.putItems(List.of(record, tombstone));

    assertThat(result).filteredOn(EventBridgeResult::isFailure).isEmpty();

    verify(eventBridgeAsyncClient, times(2)).putEvents(putEventsArgumentCaptor.capture());
    verifyNoMoreInteractions(eventBridgeAsyncClient);
    assertThat(putEventsArgumentCaptor.getAllValues()).hasSize(2);
  }

  private SinkRecord createTestRecord() {
    var valueSchema =
        SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .field("creditCard", Schema.STRING_SCHEMA)
            .field("firstName", Schema.STRING_SCHEMA)
            .field("lastName", Schema.STRING_SCHEMA)
            .field("streetAddress", Schema.STRING_SCHEMA)
            .build();

    var value = new Struct(valueSchema);
    value.put("id", "ah423k1k2");
    value.put("creditCard", "4111111111111111");
    value.put("firstName", "John");
    value.put("lastName", "Doe");
    value.put("streetAddress", "Main Street 1");

    var record = new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", valueSchema, value, 0);
    record.headers().addString("header_1", "Samwise Gamgee");
    record.headers().addString("header_2", "Gandalf");

    return record;
  }
}
