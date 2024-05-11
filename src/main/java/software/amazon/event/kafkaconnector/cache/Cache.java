/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.cache;

import java.util.function.Function;

public interface Cache<K, V> {

  V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);
}
