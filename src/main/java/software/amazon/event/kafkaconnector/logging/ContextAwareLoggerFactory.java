/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.logging;

import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

/**
 * This logger factory creates logger with a static context. Currently, each log message is prefixed
 * with the Git short commit SHA, which is generated at build time. The prefix pattern (RegEx) is
 * <code>\[@[0-9a-f]+(-dirty)?]</code>.
 *
 * @author Andreas Gebhardt
 * @since 1.2.0
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
    var gitCommitIdDescribe = properties.getOrDefault("git.commit.id.describe", "unknown-git-rev");
    context = new LogContext(String.format("[@%s] ", gitCommitIdDescribe));
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
