/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import java.io.InputStream;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesUtil {

  private static final Logger log = LoggerFactory.getLogger(PropertiesUtil.class);

  private static final String CONNECTOR_VERSION = "connector.version";
  private static final String CONNECTOR_NAME = "connector.name";
  private static final Properties properties = new Properties();

  static {
    var propertiesFile = "/EventBridgeSink.properties";
    try (InputStream stream = PropertiesUtil.class.getResourceAsStream(propertiesFile)) {
      properties.load(stream);
    } catch (Exception e) {
      log.error("Error while loading properties: ", e);
    }
  }

  public static String getConnectorVersion() {
    return properties.getProperty(CONNECTOR_VERSION);
  }

  public static String getConnectorName() {
    return properties.getProperty(CONNECTOR_NAME);
  }

  private PropertiesUtil() {}
}
