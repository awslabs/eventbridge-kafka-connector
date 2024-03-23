/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

public class DataRef {

  private final String value;

  private DataRef(final String value) {
    this.value = value;
  }

  public static DataRef of(final String value) {
    return new DataRef(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
