/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.cache;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public final class MessageDigestCacheKey {

  private final byte[] bytes;

  private MessageDigestCacheKey(byte[] bytes) {
    this.bytes = bytes;
  }

  public static MessageDigestCacheKey sha512Of(final String value) {
    try {
      var md = MessageDigest.getInstance("SHA-512");
      return new MessageDigestCacheKey(md.digest(value.getBytes()));
    } catch (final NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(bytes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    var that = (MessageDigestCacheKey) o;
    return Arrays.equals(bytes, that.bytes);
  }
}
