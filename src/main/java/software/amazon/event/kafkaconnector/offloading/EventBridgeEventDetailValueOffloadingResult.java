/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static java.util.stream.Collectors.toList;

import java.util.List;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class EventBridgeEventDetailValueOffloadingResult {

  public List<MappedSinkRecord<PutEventsRequestEntry>> success;
  public List<MappedSinkRecord<EventBridgeResult.Error>> errors;

  public EventBridgeEventDetailValueOffloadingResult(
      List<MappedSinkRecord<PutEventsRequestEntry>> success,
      List<MappedSinkRecord<EventBridgeResult.Error>> errors) {
    this.success = success;
    this.errors = errors;
  }

  public <T> List<EventBridgeResult<T>> getErrorsAsResult() {
    return this.errors.stream().map(EventBridgeResult::<T>failure).collect(toList());
  }
}
