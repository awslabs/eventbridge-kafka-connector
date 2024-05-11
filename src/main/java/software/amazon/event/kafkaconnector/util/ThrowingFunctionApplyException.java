/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

public class ThrowingFunctionApplyException extends RuntimeException {
  public ThrowingFunctionApplyException(Throwable e) {
    super(e);
  }
}
