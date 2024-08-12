/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import java.time.Instant;
import org.apache.kafka.connect.sink.SinkRecord;

public class DefaultTimeMapper implements TimeMapper {

  @Override
  public Instant getTime(SinkRecord sinkRecord) {
    // As described in AWS documentation
    // https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEventsRequestEntry.html
    // If no timestamp is provided, the timestamp of the PutEvents call is used.
    return null;
  }
}
