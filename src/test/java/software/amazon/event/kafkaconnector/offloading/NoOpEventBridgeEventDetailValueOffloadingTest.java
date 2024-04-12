/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.offloading;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

class NoOpEventBridgeEventDetailValueOffloadingTest {

  private static final EventBridgeEventDetailValueOffloadingStrategy strategy =
      new NoOpEventBridgeEventDetailValueOffloading();

  @Test
  @DisplayName("result of successful offloaded PutEventsRequestEntry should be empty")
  public void keepEmpty() {
    var actual = strategy.apply(emptyList());

    assertThat(actual.success).isEmpty();
    assertThat(actual.errors).isEmpty();
  }

  @ParameterizedTest(name = "with size {0}")
  @DisplayName("result of successful offloaded PutEventsRequestEntry should be unmodified")
  @ValueSource(longs = {1, 10, 100})
  public void keepSame(long size) {

    var items =
        Stream.generate(() -> new MappedSinkRecord<PutEventsRequestEntry>(null, null))
            .limit(size)
            .collect(toList());

    var actual = strategy.apply(items);

    assertThat(actual.success).hasSameElementsAs(items);
    assertThat(actual.errors).isEmpty();
  }
}
