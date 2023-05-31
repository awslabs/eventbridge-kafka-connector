/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.utils.Validate;

public class EventBridgeWriterRecord {
  private final SinkRecord record;
  private String errorCode;
  private String errorMessage;

  public EventBridgeWriterRecord(SinkRecord sinkRecord) {
    try {
      Validate.notNull(sinkRecord, "Sink record can't be null");
      Validate.notNull(sinkRecord.topic(), "Topic on Kafka record can't be null");
    } catch (NullPointerException e) {
      throw new DataException(e.getMessage());
    }
    this.record = sinkRecord;
  }

  public SinkRecord getSinkRecord() {
    return record;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public String getTopic() {
    return record.topic();
  }

  public Integer getPartition() {
    return record.kafkaPartition();
  }

  public Long getOffset() {
    return record.kafkaOffset();
  }

  public void setError(String errorCode, String errorMessage) {
    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  public Boolean hasError() {
    return errorCode != null;
  }
}
