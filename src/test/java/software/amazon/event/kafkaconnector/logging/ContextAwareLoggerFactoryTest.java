/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.logging;

import static ch.qos.logback.classic.Level.TRACE;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.event.Level;
import software.amazon.event.kafkaconnector.TestUtils;

class ContextAwareLoggerFactoryTest {

  private static TestUtils.ListAppender appender;

  @BeforeAll
  public static void setup() {
    appender = TestUtils.ListAppender.of(ContextAwareLoggerFactoryTest.class, TRACE);
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

  @ParameterizedTest(name = "log event with level {0}")
  @DisplayName(
      "each logging message should be prefixed with short git commit hash: \\[@[0-9a-f]+(-dirty)?]")
  @EnumSource(Level.class)
  void shouldContainGitCommitIdPrefix(Level level) {
    var logger = ContextAwareLoggerFactory.getLogger(ContextAwareLoggerFactoryTest.class);

    logger
        .makeLoggingEventBuilder(level)
        .log("Just a {} with {} arguments at level {}", "test", 3, level);

    var loggingEvents = appender.getLoggingEvents();
    assertThat(loggingEvents).hasSize(1);
    assertThat(loggingEvents.get(0).getFormattedMessage())
        .matches("\\[@[0-9a-f]+(-dirty)?] Just a test with 3 arguments at level " + level);
  }
}
