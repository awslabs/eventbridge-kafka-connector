/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.exceptions;

public class EventBridgeWriterException extends RuntimeException {

  public final ExceptionType exceptionType;

  public EventBridgeWriterException(Throwable cause, ExceptionType type) {
    super(cause);
    this.exceptionType = type;
  }

  public enum ExceptionType {
    RETRYABLE,
    NON_RETRYABLE
  }
}
