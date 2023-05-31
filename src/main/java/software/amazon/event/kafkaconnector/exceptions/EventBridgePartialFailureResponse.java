/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.exceptions;

import software.amazon.event.kafkaconnector.EventBridgeWriterRecord;

public class EventBridgePartialFailureResponse extends RuntimeException {

  public EventBridgePartialFailureResponse(EventBridgeWriterRecord eventBridgeWriterRecord) {
    super(
        String.format(
            "statusCode=%s errorMessage=%s topic=%s partition=%d offset=%d",
            eventBridgeWriterRecord.getErrorCode(),
            eventBridgeWriterRecord.getErrorMessage(),
            eventBridgeWriterRecord.getSinkRecord().topic(),
            eventBridgeWriterRecord.getSinkRecord().kafkaPartition(),
            eventBridgeWriterRecord.getSinkRecord().kafkaOffset()));
  }
}
