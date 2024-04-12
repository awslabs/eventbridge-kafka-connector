/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.cache;

import static org.assertj.core.api.Assertions.assertThat;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Test;

public class MessageDigestCacheKeyTest {

  @Test
  public void shouldImplementHashCode() {
    var one = MessageDigestCacheKey.sha512Of("some text");
    var two = MessageDigestCacheKey.sha512Of("some text");

    assertThat(one.hashCode()).isEqualTo(two.hashCode());
  }

  @Test
  public void shouldImplementEquals() {
    var one = MessageDigestCacheKey.sha512Of("some text");
    var two = MessageDigestCacheKey.sha512Of("some text");

    assertThat(one).isEqualTo(two);
  }

  @Test
  public void equalsContract() {
    EqualsVerifier.forClass(MessageDigestCacheKey.class).verify();
  }
}
