/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import java.util.Objects;
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

  @Override
  public int hashCode() {
    return Objects.hash(sinkRecord, value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    var that = (MappedSinkRecord<?>) o;
    return Objects.equals(sinkRecord, that.sinkRecord) && Objects.equals(value, that.value);
  }
}
