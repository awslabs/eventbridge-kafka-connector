/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.eventbridge.model.EventBridgeException;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;
import software.amazon.event.kafkaconnector.exceptions.EventBridgeWriterException;

@ExtendWith(MockitoExtension.class)
public class EventBridgeWriterTest {

  @Mock private EventBridgeAsyncClient eventBridgeAsyncClient;

  @Test
  public void returnsSuccessfulRecords() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                PutEventsResponse.builder().failedEntryCount(0).build()));

    var myWriter = new EventBridgeWriter(eventBridgeAsyncClient, mock(EventBridgeSinkConfig.class));
    var eventBridgeWriterRecord = new EventBridgeWriterRecord(createTestRecord());
    var eventBridgeWriterRecord2 = new EventBridgeWriterRecord(createTestRecord());
    var result = myWriter.putItems(List.of(eventBridgeWriterRecord, eventBridgeWriterRecord2));

    assertThat(result.get(0).hasError(), is(false));
    assertThat(result.get(1).hasError(), is(false));
  }

  @Test
  public void throwsWriterExceptionOnEventBridgeError() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenThrow(EventBridgeException.class);

    var myWriter = new EventBridgeWriter(eventBridgeAsyncClient, mock(EventBridgeSinkConfig.class));
    var eventBridgeWriterRecord = new EventBridgeWriterRecord(createTestRecord());

    assertThrows(
        EventBridgeWriterException.class,
        () -> {
          myWriter.putItems(List.of(eventBridgeWriterRecord));
        });
  }

  @Test
  public void populatesPartialErrors() {
    var properEntry = PutEventsResultEntry.builder().eventId(UUID.randomUUID().toString()).build();
    var failedEntry =
        PutEventsResultEntry.builder().errorMessage("Failed").errorCode("500").build();
    var partiallyFailedEBResponse =
        CompletableFuture.completedFuture(
            PutEventsResponse.builder()
                .failedEntryCount(1)
                .entries(List.of(properEntry, failedEntry))
                .build());
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(partiallyFailedEBResponse);

    var myWriter = new EventBridgeWriter(eventBridgeAsyncClient, mock(EventBridgeSinkConfig.class));
    var eventBridgeWriterRecord = new EventBridgeWriterRecord(createTestRecord());
    var eventBridgeWriterRecord2 = new EventBridgeWriterRecord(createTestRecord());
    var result = myWriter.putItems(List.of(eventBridgeWriterRecord, eventBridgeWriterRecord2));

    assertThat(result.get(0).getErrorCode(), is(nullValue()));
    assertThat(result.get(1).getErrorCode(), equalTo("500"));
    assertThat(result.get(1).getErrorMessage(), equalTo("Failed"));
  }

  @Test
  public void populatesPartialDataException() {
    when(eventBridgeAsyncClient.putEvents(any(PutEventsRequest.class)))
        .thenReturn(
            CompletableFuture.completedFuture(
                PutEventsResponse.builder().failedEntryCount(0).build()));

    var validRecord = new EventBridgeWriterRecord(createTestRecord());
    SinkRecord tombstone =
        new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "somekey", Schema.STRING_SCHEMA, null, 0);
    var tombstoneRecord = new EventBridgeWriterRecord(tombstone);

    var writer = new EventBridgeWriter(eventBridgeAsyncClient, mock(EventBridgeSinkConfig.class));
    var result = writer.putItems(List.of(validRecord, tombstoneRecord));

    assertThat(result.get(0).getErrorCode(), is(nullValue()));
    assertThat(result.get(1).getErrorCode(), is(nullValue()));
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
