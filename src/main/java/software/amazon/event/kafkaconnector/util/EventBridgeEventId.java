/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

public class EventBridgeEventId {

  private final String value;

  EventBridgeEventId(final String value) {
    this.value = value;
  }

  public static EventBridgeEventId of(final PutEventsResultEntry entry) {
    return new EventBridgeEventId(entry.eventId());
  }

  @Override
  public String toString() {
    return value;
  }
}
