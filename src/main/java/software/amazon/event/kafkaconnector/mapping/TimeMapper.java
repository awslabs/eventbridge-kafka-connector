/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import java.time.Instant;
import org.apache.kafka.connect.sink.SinkRecord;

public interface TimeMapper {
  Instant getTime(SinkRecord sinkRecord);
}
