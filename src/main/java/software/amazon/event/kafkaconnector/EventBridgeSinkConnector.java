/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.CONFIG_DEF;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.util.PropertiesUtil;

public class EventBridgeSinkConnector extends SinkConnector {

  private final Logger log = ContextAwareLoggerFactory.getLogger(EventBridgeSinkConnector.class);

  private Map<String, String> originalProps;

  @Override
  public void start(Map<String, String> properties) {
    this.originalProps = properties;
  }

  @Override
  public Class<? extends Task> taskClass() {
    return EventBridgeSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} tasks.", maxTasks);
    ArrayList<Map<String, String>> taskConfigs = new ArrayList<>();
    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(originalProps);
    }
    return taskConfigs;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    var config = super.validate(connectorConfigs);
    EventBridgeSinkConfigValidator.validate(config);
    return config;
  }

  @Override
  public String version() {
    return PropertiesUtil.getConnectorVersion();
  }

  @Override
  public void stop() {
    log.info("Stopping sink connector");
  }
}
