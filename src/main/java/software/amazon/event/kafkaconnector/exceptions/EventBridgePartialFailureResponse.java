/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.exceptions;

import org.apache.kafka.connect.sink.SinkRecord;

public class EventBridgePartialFailureResponse extends RuntimeException {

  public EventBridgePartialFailureResponse(SinkRecord sinkRecord, String message, Throwable cause) {
    super(
        String.format(
            "errorMessage=%s topic=%s partition=%d offset=%d cause=%s",
            message,
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset(),
            cause));
  }
}
