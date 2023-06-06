/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import org.apache.kafka.connect.sink.SinkRecord;

public class MappedSinkRecord<T> {

  private final SinkRecord sinkRecord;
  private final T value;

  public MappedSinkRecord(SinkRecord sinkRecord, T value) {
    this.sinkRecord = sinkRecord;
    this.value = value;
  }

  public SinkRecord getSinkRecord() {
    return sinkRecord;
  }

  public T getValue() {
    return value;
  }
}
