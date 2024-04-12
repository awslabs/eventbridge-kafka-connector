/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.cache;

import static java.util.stream.IntStream.rangeClosed;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Random;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class FifoCacheTest {

  @Test
  public void shouldRequireMaxSizeOne() {
    var exception =
        assertThrows(IllegalArgumentException.class, () -> new FifoCache<Integer, String>(0));

    assertThat(exception).hasMessage("Maximum size must be at least one.");
    assertThat(new FifoCache<Integer, String>(1)).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 8, 16})
  public void shouldKeepUpToMaxSize(int maxSize) {

    var cache = new FifoCache<Integer, Integer>(maxSize);

    // fill cache up to maximum
    var values = randomInts(maxSize);
    rangeClosed(1, maxSize)
        .forEach(
            key -> {
              var value = values[key];
              assertThat(cache.computeIfAbsent(key, (ignore) -> value)).isEqualTo(value);
            });

    // check that each entry is not updated
    rangeClosed(1, maxSize)
        .forEach(
            key -> {
              var value = values[key];
              assertThat(cache.computeIfAbsent(key, (ignore) -> rnd.nextInt())).isEqualTo(value);
            });
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 4, 8, 16})
  public void shouldReplaceFirstEntry(int maxSize) {

    var cache = new FifoCache<Integer, Integer>(maxSize);

    // fill cache up to maximum
    var values = randomInts(maxSize);
    rangeClosed(1, maxSize)
        .forEach(
            key -> {
              var value = values[key];
              assertThat(cache.computeIfAbsent(key, (ignore) -> value)).isEqualTo(value);
            });

    // add entry into full cache
    cache.computeIfAbsent(maxSize + 1, (ignore) -> 0);

    assertThat(cache.computeIfAbsent(maxSize + 1, (ignore) -> maxSize + 1)).isEqualTo(0);

    // check that entries with key in [2...maxsize] are not touched
    rangeClosed(2, maxSize)
        .forEach(
            key -> {
              var value = values[key];
              assertThat(cache.computeIfAbsent(key, (ignore) -> rnd.nextInt())).isEqualTo(value);
            });

    // check that FIRST entry with key=1 is re-added as a result of replacement for (add) new entry
    // to full cache
    assertThat(cache.computeIfAbsent(1, (ignore) -> maxSize + 2))
        .isNotEqualTo(values[1])
        .isEqualTo(maxSize + 2);
  }

  private static final Random rnd = new Random();

  private static int[] randomInts(int size) {
    return rnd.ints(size + 1).toArray();
  }
}
