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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.TestUtils.ListAppender;

public class StatusReporterTest {

  private static ListAppender appender;

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  @BeforeAll
  public static void setup() {
    appender = ListAppender.of(StatusReporter.class, TRACE);
    appender.start();
  }

  @AfterAll
  public static void teardown() {
    appender.detach();
  }

  @AfterEach
  public void clearLoggingEvents() {
    appender.clear();
  }

  @Test
  @DisplayName("Status Reporter should log message of total sent records")
  public void shouldContainLoggingEvent() {
    var sut = new StatusReporter(100, MILLISECONDS);
    sut.startAsync();

    var task = scheduler.scheduleAtFixedRate(() -> sut.setSentRecords(1), 175, 75, MILLISECONDS);

    await()
        .atMost(200, SECONDS)
        .untilAsserted(
            () ->
                assertThat(appender.getLoggingEvents())
                    .extracting("level", "formattedMessage")
                    .containsSubsequence(
                        new Tuple(Level.INFO, "Starting status reporter"),
                        new Tuple(Level.INFO, "Total records sent=0"),
                        new Tuple(Level.INFO, "Total records sent=1"),
                        new Tuple(Level.INFO, "Total records sent=2")));

    task.cancel(false);
    sut.stopAsync();
  }
}
