/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.cache;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class FifoCache<K, V> implements Cache<K, V> {

  private static class SizeLimitedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {

    private final int maxSize;

    public SizeLimitedLinkedHashMap(int maxSize) {
      super(maxSize);
      this.maxSize = maxSize;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > maxSize;
    }
  }

  private final HashMap<K, V> delegate;

  public FifoCache(int maxSize) {
    if (maxSize < 1) {
      throw new IllegalArgumentException("Maximum size must be at least one.");
    }
    this.delegate = new SizeLimitedLinkedHashMap<>(maxSize);
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return delegate.computeIfAbsent(key, mappingFunction);
  }
}
