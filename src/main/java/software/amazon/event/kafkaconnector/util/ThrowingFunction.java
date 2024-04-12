/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.util;

import java.util.function.Function;

@FunctionalInterface
public interface ThrowingFunction<T, R> {

  R apply(T t) throws Throwable;

  static <T, R> Function<T, R> wrap(ThrowingFunction<T, R> f) {
    return t -> {
      try {
        return f.apply(t);
      } catch (Throwable e) {
        throw new ThrowingFunctionApplyException(e);
      }
    };
  }
}
