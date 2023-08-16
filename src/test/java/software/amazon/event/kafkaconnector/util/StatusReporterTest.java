/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import static ch.qos.logback.classic.Level.TRACE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

public class StatusReporterTest {

  private static List<ILoggingEvent> loggingEvents;

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  @BeforeAll
  public static void setup() {
    var appender = new ListAppender<ILoggingEvent>();
    appender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
    appender.start();
    var logger = (Logger) LoggerFactory.getLogger(StatusReporter.class);
    logger.setLevel(TRACE);
    logger.addAppender(appender);
    loggingEvents = appender.list;
  }

  @AfterEach
  public void clearLoggingEvents() {
    loggingEvents.clear();
  }

  @Test
  @DisplayName("Status Reporter should log message of total sent records")
  public void shouldContainLoggingEvent() {
    var sut = new StatusReporter(100, MILLISECONDS);
    sut.startAsync();

    scheduler.scheduleAtFixedRate(() -> sut.setSentRecords(1), 175, 75, MILLISECONDS);

    await()
        .atMost(2, SECONDS)
        .untilAsserted(
            () ->
                assertThat(loggingEvents)
                    .extracting("level", "formattedMessage")
                    .containsSubsequence(
                        new Tuple(Level.INFO, "Starting status reporter"),
                        new Tuple(Level.INFO, "Total records sent=0"),
                        new Tuple(Level.INFO, "Total records sent=1"),
                        new Tuple(Level.INFO, "Total records sent=2")));

    sut.stopAsync();
  }
}
