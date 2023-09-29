/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import static ch.qos.logback.classic.Level.INFO;
import static ch.qos.logback.classic.Level.TRACE;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.regex.Pattern;
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
                    .filteredOn(level(INFO))
                    .extracting(ILoggingEvent::getFormattedMessage)
                    .contains("Starting status reporter")
                    .filteredOn(startsWith("Total records sent="))
                    .hasSizeGreaterThan(2)
                    .isSortedAccordingTo(matchGroupOf("Total records sent=(\\d+)")));

    task.cancel(false);
    sut.stopAsync();
  }

  private static Predicate<? super ILoggingEvent> level(Level expectedLevel) {
    return (event) -> event.getLevel() == expectedLevel;
  }

  private static Predicate<? super String> startsWith(String prefix) {
    return (value) -> value.startsWith(prefix);
  }

  private static Comparator<String> matchGroupOf(final String regex) {
    var pattern = Pattern.compile(regex);
    return Comparator.comparing(
        it -> {
          var matcher = pattern.matcher(it);
          return matcher.find() ? parseInt(matcher.group(1)) : -1;
        });
  }
}
