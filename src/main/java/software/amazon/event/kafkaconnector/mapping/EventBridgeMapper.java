/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;

@FunctionalInterface
public interface EventBridgeMapper {

  EventBridgeMappingResult map(List<SinkRecord> records);
}
