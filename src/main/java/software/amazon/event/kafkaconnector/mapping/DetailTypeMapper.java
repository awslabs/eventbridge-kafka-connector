/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public interface DetailTypeMapper {
  String getDetailType(SinkRecord record);

  void configure(EventBridgeSinkConfig eventBridgeSinkConfig);
}
