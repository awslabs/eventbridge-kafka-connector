/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.empty;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import software.amazon.event.kafkaconnector.exceptions.EventBridgePartialFailureResponse;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;
import software.amazon.event.kafkaconnector.util.PropertiesUtil;
import software.amazon.event.kafkaconnector.util.StatusReporter;

public class EventBridgeSinkTask extends SinkTask {

  private final Logger log = ContextAwareLoggerFactory.getLogger(EventBridgeSinkTask.class);
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

    var remainingRecords = List.copyOf(records);

    var attempts = new AtomicInteger(0);
    var start = OffsetDateTime.now(ZoneOffset.UTC);

    while (!remainingRecords.isEmpty()) {
      attempts.incrementAndGet();
      log.trace(
          "putItems call started: start={} attempts={} maxRetries={}",
          start,
          attempts,
          config.maxRetries);

      remainingRecords =
          eventBridgeWriter.putItems(remainingRecords).stream()
              .filter(EventBridgeResult::isFailure)
              .flatMap(it -> handleFailedEntries(it.failure(), attempts.get()))
              .collect(toList());

      // only report successful putEvents calls
      statusReporter.setSentRecords(records.size());

      try {
        // TODO: improve retry delay implementation
        TimeUnit.MILLISECONDS.sleep(config.retriesDelay);
      } catch (InterruptedException ie) {
        throw new ConnectException(ie);
      }
    }

    var completion = OffsetDateTime.now(ZoneOffset.UTC);
    log.trace(
        "putItems call completed: start={} completion={} durationMillis={} "
            + "attempts={} "
            + "maxRetries={}",
        start,
        completion,
        Duration.between(start, completion).toMillis(),
        attempts,
        config.maxRetries);
  }

  private Stream<SinkRecord> handleFailedEntries(
      MappedSinkRecord<EventBridgeResult.Error> errorRecord, Integer attempts) {

    var error = errorRecord.getValue();
    var message = error.getMessage();
    var cause = error.getCause();
    switch (error.getType()) {
      case REPORT_ONLY:
        var failure =
            new EventBridgePartialFailureResponse(errorRecord.getSinkRecord(), message, cause);
        if (dlq != null) {
          log.trace("Sending record to dead-letter queue: {}", errorRecord);
          dlq.report(errorRecord.getSinkRecord(), failure);
        } else {
          log.warn("Dead-letter queue not configured: skipping failed record", failure);
        }
        return empty();

      case RETRY:
        if (attempts > config.maxRetries) {
          log.error(
              "Not retrying failed putItems call: reached max retries attempts={} maxRetries={} "
                  + "errorMessage={}",
              attempts,
              config.maxRetries,
              message,
              cause);
          throw new ConnectException(cause);
        }

        log.warn(
            "Retrying failed putItems call: attempts={} maxRetries={} errorMessage={}",
            attempts,
            config.maxRetries,
            message,
            cause);

        return Stream.of(errorRecord.getSinkRecord());

      case PANIC:
        log.error(
            "Non-retryable failed put call: failing connector errorMessage={}", message, cause);
        throw new ConnectException(cause);
    }
    return Stream.empty();
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
