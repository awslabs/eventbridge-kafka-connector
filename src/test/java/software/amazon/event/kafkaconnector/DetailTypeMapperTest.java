/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import software.amazon.event.kafkaconnector.mapping.DefaultDetailTypeMapper;

public class DetailTypeMapperTest {

  @Test
  public void validDetailType() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "test",
            "aws.eventbridge.connector.id", "test-id");

    var detailTypeMapper = new DefaultDetailTypeMapper();
    detailTypeMapper.configure(new EventBridgeSinkConfig(myConfig));

    assertThat(detailTypeMapper.getDetailType(createSinkRecordWithTopic("topic1")), is("test"));
    assertThat(detailTypeMapper.getDetailType(createSinkRecordWithTopic("topic2")), is("test"));
  }

  @Test
  public void genericTopicResolver() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "my-first-${topic}",
            "aws.eventbridge.connector.id", "test-id");

    var detailTypeMapper = new DefaultDetailTypeMapper();
    detailTypeMapper.configure(new EventBridgeSinkConfig(myConfig));

    assertThat(
        detailTypeMapper.getDetailType(createSinkRecordWithTopic("something")),
        is("my-first-something"));
  }

  @Test
  public void multipleTopicsResolve() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "topic1:something,topic2:something-else",
            "aws.eventbridge.connector.id", "test-id");

    var detailTypeMapper = new DefaultDetailTypeMapper();
    detailTypeMapper.configure(new EventBridgeSinkConfig(myConfig));

    assertThat(
        detailTypeMapper.getDetailType(createSinkRecordWithTopic("topic1")), is("something"));
    assertThat(
        detailTypeMapper.getDetailType(createSinkRecordWithTopic("topic2")), is("something-else"));
  }

  @Test
  public void ifTopicNotPresentInListShouldReturnDefault() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "topic1:something,topic2:something-else",
            "aws.eventbridge.connector.id", "test-id");

    var detailTypeMapper = new DefaultDetailTypeMapper();
    detailTypeMapper.configure(new EventBridgeSinkConfig(myConfig));

    assertThat(
        detailTypeMapper.getDetailType(createSinkRecordWithTopic("topic3")),
        is("kafka-connect-topic3"));
  }

  @Test
  public void singleTopicDefinition() {
    var myConfig =
        Map.of(
            "aws.eventbridge.region", "eu-central-1",
            "aws.eventbridge.eventbus.arn", "something",
            "aws.eventbridge.detail.types", "topic1:something",
            "aws.eventbridge.connector.id", "test-id");

    var detailTypeMapper = new DefaultDetailTypeMapper();
    detailTypeMapper.configure(new EventBridgeSinkConfig(myConfig));

    assertThat(
        detailTypeMapper.getDetailType(createSinkRecordWithTopic("topic1")), is("something"));
    assertThat(
        detailTypeMapper.getDetailType(createSinkRecordWithTopic("topic3")),
        is("kafka-connect-topic3"));
  }

  private SinkRecord createSinkRecordWithTopic(String topic) {
    return new SinkRecord(topic, 0, STRING_SCHEMA, "key", null, "", 0);
  }
}
