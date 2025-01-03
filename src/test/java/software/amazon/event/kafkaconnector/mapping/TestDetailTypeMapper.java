/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class TestDetailTypeMapper implements DetailTypeMapper {
  public static final String DETAIL_TYPE = "TestDetailType";

  @Override
  public String getDetailType(SinkRecord record) {
    return DETAIL_TYPE;
  }

  @Override
  public void configure(EventBridgeSinkConfig eventBridgeSinkConfig) {}
}
