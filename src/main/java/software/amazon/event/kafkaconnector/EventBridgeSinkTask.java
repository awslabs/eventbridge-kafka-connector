/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import com.google.common.collect.Lists;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.event.kafkaconnector.exceptions.EventBridgePartialFailureResponse;
import software.amazon.event.kafkaconnector.exceptions.EventBridgeWriterException;
import software.amazon.event.kafkaconnector.util.PropertiesUtil;
import software.amazon.event.kafkaconnector.util.StatusReporter;

public class EventBridgeSinkTask extends SinkTask {

  private final Logger log = LoggerFactory.getLogger(EventBridgeSinkTask.class);
  private EventBridgeWriter eventBridgeWriter;
  private ErrantRecordReporter dlq;
  private EventBridgeSinkConfig config;
  private final StatusReporter statusReporter = new StatusReporter(1, TimeUnit.MINUTES);

  @Override
  public void start(Map<String, String> properties) {
    log.info("Starting Kafka Connect task for EventBridgeSinkConnector");

    var config = new EventBridgeSinkConfig(properties);
    var eventBridgeWriter = new EventBridgeWriter(config);

    ErrantRecordReporter dlq;
    try {
      dlq = context.errantRecordReporter();
    } catch (NoSuchMethodError | NoClassDefFoundError e) {
      dlq = null;
    }
    if (dlq != null) {
      log.info("Dead-letter queue enabled");
    }

    startInternal(config, eventBridgeWriter, dlq);
  }

  void startInternal(
      EventBridgeSinkConfig config, EventBridgeWriter eventBridgeWriter, ErrantRecordReporter dlq) {
    this.config = config;
    this.eventBridgeWriter = eventBridgeWriter;
    this.dlq = dlq;
    statusReporter.startAsync();
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.trace("EventBridgeSinkTask put called with {} records: {}", records.size(), records);
    if (records.size() == 0) {
      log.trace("Returning early: 0 records received");
      return;
    }

    var eventBridgeWriterRecords =
        records.stream()
            .map(this::convertToEventBridgeWriterRecord)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // TODO: disabling batching for now until we can correctly calculate the size of the whole batch
    // to avoid failing all entries in an EventBridge request due to exceeding 256kb size limit
    var batches = Lists.partition(eventBridgeWriterRecords, 1);

    var attempts = 0;
    var start = OffsetDateTime.now(ZoneOffset.UTC);
    while (true) {
      attempts++;
      log.trace(
          "EventBridgeSinkTask putItems call started: start={} attempts={} maxRetries={}",
          start,
          attempts,
          config.maxRetries);
      try {
        batches.stream()
            .flatMap(batch -> eventBridgeWriter.putItems(batch).stream())
            .filter(EventBridgeWriterRecord::hasError)
            .forEach(this::handleFailedEntries);
        // only report successful putEvents calls
        statusReporter.setSentRecords(eventBridgeWriterRecords.size());
      } catch (EventBridgeWriterException e) {
        switch (e.exceptionType) {
          case RETRYABLE:
            handleRetryableError(attempts, e);
            continue; // retry
          case NON_RETRYABLE:
            handleNonRetryableError(e);
        }
      }
      var completion = OffsetDateTime.now(ZoneOffset.UTC);
      log.trace(
          "EventBridgeSinkTask putItems call completed: start={} completion={} durationMillis={} "
              + "attempts={} "
              + "maxRetries={}",
          start,
          completion,
          Duration.between(start, completion).toMillis(),
          attempts,
          config.maxRetries);
      return;
    }
  }

  private void handleNonRetryableError(EventBridgeWriterException e) {
    log.error("Non-retryable failed put call: failing connector errorMessage={}", e.getMessage());
    throw new ConnectException(e);
  }

  private void handleRetryableError(Integer attempts, EventBridgeWriterException e) {
    if (attempts > config.maxRetries) {
      log.error(
          "Not retrying failed putItems call: reached max retries attempts={} maxRetries={} "
              + "errorMessage={}",
          attempts,
          config.maxRetries,
          e.getMessage());
      throw new ConnectException(e);
    }

    try {
      log.warn(
          "Retrying failed putItems call: attempts={} maxRetries={} errorMessage={}",
          attempts,
          config.maxRetries,
          e.getMessage());
      // TODO: improve retry delay implementation
      TimeUnit.MILLISECONDS.sleep(config.retriesDelay);
    } catch (InterruptedException ie) {
      throw new ConnectException(ie);
    }
  }

  private void handleFailedEntries(EventBridgeWriterRecord record) {
    var failure = new EventBridgePartialFailureResponse(record);
    if (dlq != null) {
      log.trace("Sending record to dead-letter queue: {}", record);
      dlq.report(record.getSinkRecord(), failure);
    } else {
      log.warn("Dead-letter queue not configured: skipping failed record", failure);
    }
  }

  private EventBridgeWriterRecord convertToEventBridgeWriterRecord(SinkRecord record) {
    EventBridgeWriterRecord eventBridgeRecord = null;
    try {
      eventBridgeRecord = new EventBridgeWriterRecord(record);
    } catch (DataException e) {
      log.error("Invalid record detected: skipping record", e);
    }
    return eventBridgeRecord;
  }

  @Override
  public String version() {
    return PropertiesUtil.getConnectorVersion();
  }

  @Override
  public void stop() {
    log.trace("Stopping sink task");
    if (eventBridgeWriter != null) {
      eventBridgeWriter.shutDownEventBridgeClient();
    }

    if (statusReporter.isRunning()) {
      statusReporter.stopAsync();
    }
  }
}
