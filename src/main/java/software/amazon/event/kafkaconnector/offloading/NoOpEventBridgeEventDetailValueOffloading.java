/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static java.util.Collections.emptyList;

import java.util.List;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.event.kafkaconnector.mapping.EventBridgeMappingResult;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class NoOpEventBridgeEventDetailValueOffloading
    implements EventBridgeEventDetailValueOffloadingStrategy {

  @Override
  public EventBridgeMappingResult apply(
      List<MappedSinkRecord<PutEventsRequestEntry>> putEventsRequestEntries) {
    return new EventBridgeMappingResult(putEventsRequestEntries, emptyList());
  }
}
