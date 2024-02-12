/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.AWS_DETAIL_TYPES_DEFAULT;

import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class DefaultDetailTypeMapper implements DetailTypeMapper {

  private EventBridgeSinkConfig eventBridgeSinkConfig;

  @Override
  public String getDetailType(SinkRecord sinkRecord) {
    var detailType = eventBridgeSinkConfig.detailType;
    if (detailType != null) return detailType.replace("${topic}", sinkRecord.topic());
    return eventBridgeSinkConfig.detailTypeByTopic.getOrDefault(
        sinkRecord.topic(), AWS_DETAIL_TYPES_DEFAULT.replace("${topic}", sinkRecord.topic()));
  }

  @Override
  public void configure(EventBridgeSinkConfig eventBridgeSinkConfig) {
    this.eventBridgeSinkConfig = eventBridgeSinkConfig;
  }
}
