/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import java.util.List;
import java.util.stream.Stream;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

@FunctionalInterface
public interface EventBridgeBatchingStrategy {

  Stream<List<MappedSinkRecord<PutEventsRequestEntry>>> apply(
      Stream<MappedSinkRecord<PutEventsRequestEntry>> records);
}
