/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.logging;

import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * This logger factory creates logger with a static context. Currently, each log message is prefixed
 * with the Git information of <code>git describe --abbrev --always --dirty</code>. The prefix
 * pattern (RegEx) is <code>\[@[0-9a-f]{7}(-dirty)?]</code>.
 *
 * @author Andreas Gebhardt
 * @since 1.3.0
 */
public class ContextAwareLoggerFactory {

  private static final LogContext context;

  static {
    var properties = new Properties();
    try {
      var resource = ContextAwareLoggerFactory.class.getResource("/git.properties");
      if (resource != null) {
        try (var stream = resource.openStream()) {
          properties.load(stream);
        }
      }
    } catch (NullPointerException | IOException ignore) {
    }
    context =
        new LogContext(
            Optional.ofNullable(properties.get("git.commit.id.abbrev"))
                .map(
                    id ->
                        String.format(
                            "[@%s%s] ",
                            id, "true".equals(properties.get("git.dirty")) ? "-dirty" : ""))
                .orElse("unknown-git-rev"));
  }

  /**
   * Return a logger named corresponding to the class passed as parameter and the static context
   * wich prefix each log message.
   *
   * @param clazz the returned logger will be named after clazz
   * @return logger
   */
  public static Logger getLogger(Class<?> clazz) {
    return context.logger(clazz);
  }
}
