/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static org.hamcrest.Matchers.*;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.model.Message;

@TestMethodOrder(OrderAnnotation.class)
public class EventBridgeSinkConnectorIT extends AbstractEventBridgeSinkConnectorIT {

  private static final String TEST_EVENT_KEY = "eventbridge-e2e";

  @Test
  @Order(1)
  public void startConnector() throws IOException, InterruptedException {
    startConnector(Path.of("e2e/connect-config.json"), "eventbridge-e2e");
  }

  @Test
  @Order(2)
  public void sendJsonRecordToKafkaReceiveFromSQS() {
    log.info("creating kafka producer");

    var mapper = new ObjectMapper();

    var sentTimestamp = LocalDateTime.now().toString();
    var jsonTestEvent =
        mapper
            .createObjectNode()
            .put("sentTimestamp", sentTimestamp)
            .put("message", "hello from kafka");

    // use as marker object
    putS3Object("START", RequestBody.empty());

    log.info("sending kafka json test record {} to topic {}", jsonTestEvent, TEST_RESOURCE_NAME);
    sendKafkaRecord(new ProducerRecord<>(TEST_RESOURCE_NAME, TEST_EVENT_KEY, jsonTestEvent));

    log.info("polling sqs queue {} for json test record", getQueueArn());
    await()
        .atMost(5, SECONDS)
        .pollInterval(1, SECONDS)
        .untilAsserted(
            () -> {
              var messages = receiveSqsMessages(1).messages();
              log.info("received sqs message(s): {}", messages);

              assertThat(messages)
                  .hasSize(1)
                  .extracting(Message::body)
                  .satisfies(
                      body ->
                          MatcherAssert.assertThat(
                              body,
                              isJson(
                                  withJsonPath(
                                      "$.detail",
                                      allOf(
                                          hasJsonPath("partition", greaterThanOrEqualTo(0)),
                                          hasJsonPath("offset", greaterThanOrEqualTo(0)),
                                          hasJsonPath("topic", equalTo(TEST_RESOURCE_NAME)),
                                          hasJsonPath("key", equalTo(TEST_EVENT_KEY)),
                                          hasJsonPath(
                                              "value.sentTimestamp", equalTo(sentTimestamp)),
                                          hasJsonPath(
                                              "value.message", equalTo("hello from kafka")))))),
                      atIndex(0));
            });

    // use as marker object
    putS3Object("STOP", RequestBody.empty());

    var s3Objects = listS3Objects().contents();
    log.info("received s3 object(s): {}", s3Objects);

    assertThat(s3Objects)
        .extracting(S3Object::key)
        // order is not ensured by LocalStack
        .containsExactlyInAnyOrder("START", "STOP");
  }

  @Order(3)
  @Test
  public void connectorTaskIsRunning() throws IOException, InterruptedException {
    assertThat(connectorStateOfTask(0)).isEqualTo("RUNNING");
  }
}
