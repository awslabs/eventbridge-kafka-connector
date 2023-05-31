/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class EventBridgeSinkConfigTest {

  @Test
  public void validDetailType() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "test",
            "aws.eventbridge.connector.id", "test-id");

    var config = new EventBridgeSinkConfig(myConfig);

    assertThat(config.getDetailType("topic1"), is("test"));
    assertThat(config.getDetailType("topic2"), is("test"));
  }

  @Test
  public void genericTopicResolver() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "my-first-${topic}",
            "aws.eventbridge.connector.id", "test-id");

    var config = new EventBridgeSinkConfig(myConfig);

    assertThat(config.getDetailType("something"), is("my-first-something"));
  }

  @Test
  public void multipleTopicsResolve() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "topic1:something,topic2:something-else",
            "aws.eventbridge.connector.id", "test-id");

    var config = new EventBridgeSinkConfig(myConfig);

    assertThat(config.getDetailType("topic1"), is("something"));
    assertThat(config.getDetailType("topic2"), is("something-else"));
  }

  @Test
  public void ifTopicNotPresentInListShouldReturnDefault() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "topic1:something,topic2:something-else",
            "aws.eventbridge.connector.id", "test-id");

    var config = new EventBridgeSinkConfig(myConfig);

    assertThat(config.getDetailType("topic3"), is("kafka-connect-topic3"));
  }

  @Test
  public void singleTopicDefinition() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "topic1:something",
            "aws.eventbridge.connector.id", "test-id");

    var config = new EventBridgeSinkConfig(myConfig);

    assertThat(config.getDetailType("topic1"), is("something"));
    assertThat(config.getDetailType("topic3"), is("kafka-connect-topic3"));
  }
}
