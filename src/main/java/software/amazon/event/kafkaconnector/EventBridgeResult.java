/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.PANIC;
import static software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.REPORT_ONLY;
import static software.amazon.event.kafkaconnector.EventBridgeResult.ErrorType.RETRY;

import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class EventBridgeResult<T> {

  public enum ErrorType {
    REPORT_ONLY,
    RETRY,
    PANIC
  }

  public static class Error {

    private final ErrorType type;
    private final String message;
    private final Throwable cause;

    private Error(final ErrorType type, final String message, final Throwable cause) {
      this.message = message;
      this.cause = cause;
      this.type = type;
    }

    public static Error reportOnly(final String message, final Throwable cause) {
      return new Error(REPORT_ONLY, message, cause);
    }

    public static Error reportOnly(final String message) {
      return new Error(REPORT_ONLY, message, null);
    }

    public static Error retry(final Throwable cause) {
      return new Error(RETRY, "", cause);
    }

    public static Error panic(final Throwable cause) {
      return new Error(PANIC, "", cause);
    }

    public ErrorType getType() {
      return type;
    }

    public String getMessage() {
      return message;
    }

    public Throwable getCause() {
      return cause;
    }
  }

  private static class Failure {

    private final MappedSinkRecord<Error> value;

    Failure(SinkRecord sinkRecord, Error error) {
      value = new MappedSinkRecord<>(sinkRecord, error);
    }
  }

  private final Object value;

  private EventBridgeResult(Object value) {
    this.value = value;
  }

  public static <T> EventBridgeResult<T> success(SinkRecord sinkRecord, T value) {
    return new EventBridgeResult<>(new MappedSinkRecord<>(sinkRecord, value));
  }

  public static <T> EventBridgeResult<T> failure(SinkRecord sinkRecord, Error error) {
    return new EventBridgeResult<>(new Failure(sinkRecord, error));
  }

  public static <T> EventBridgeResult<T> failure(MappedSinkRecord<Error> value) {
    return new EventBridgeResult<>(new Failure(value.getSinkRecord(), value.getValue()));
  }

  public boolean isSuccess() {
    return !isFailure();
  }

  public boolean isFailure() {
    return value != null && Failure.class.equals(value.getClass());
  }

  @SuppressWarnings("unchecked")
  public MappedSinkRecord<T> success() {
    if (isFailure())
      throw new IllegalStateException("Result is a failure and cannot be accessed as success.");
    return (MappedSinkRecord<T>) value;
  }

  public MappedSinkRecord<Error> failure() {
    if (isSuccess())
      throw new IllegalStateException("Result is a success and cannot be accessed as failure.");
    return ((Failure) value).value;
  }
}
