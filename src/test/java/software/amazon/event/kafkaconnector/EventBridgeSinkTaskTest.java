/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.event.kafkaconnector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;

@ExtendWith(MockitoExtension.class)
public class EventBridgeSinkTaskTest {

  @Mock private EventBridgeAsyncClient eventBridgeAsyncClient;

  @Test
  public void returnsSuccessfulRecords() {
    // Todo: Add tests for SinkTask
  }
}
