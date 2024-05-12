/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasNoJsonPath;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.isJson;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.withJsonPath;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.model.Message;

@TestMethodOrder(OrderAnnotation.class)
public class EventBridgeSinkConnectorS3IT extends AbstractEventBridgeSinkConnectorIT {

  private static final String TEST_EVENT_KEY = "eventbridge-e2e-s3";

  @Test
  @Order(1)
  public void startConnector() throws IOException, InterruptedException {
    startConnector(Path.of("e2e/connect-config-s3.json"), "eventbridge-e2e-s3");
  }

  @Test
  @Order(2)
  public void sendJsonRecordToKafkaReceiveFromSQS() {
    log.info("creating kafka producer");

    var mapper = new ObjectMapper();

    var sendTimestamp = LocalDateTime.now().toString();
    var jsonTestEvent =
        mapper
            .createObjectNode()
            .put("sentTimestamp", sendTimestamp)
            .put("message", "hello from kafka");

    // use as marker object
    putS3Object("START", RequestBody.empty());

    log.info("sending kafka json test record {} to topic {}", jsonTestEvent, TEST_RESOURCE_NAME);
    sendKafkaRecord(new ProducerRecord<>(TEST_RESOURCE_NAME, TEST_EVENT_KEY, jsonTestEvent));

    log.info("polling sqs queue {} for json test record", getQueueArn());
    var s3ObjectArn = new AtomicReference<String>(null);
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
                                              "value.sentTimestamp", equalTo(sendTimestamp)),
                                          hasNoJsonPath("value.message"),
                                          hasJsonPath(
                                              "dataref", startsWith("arn:aws:s3:::test-bucket/")),
                                          hasJsonPath(
                                              "datarefJsonPath",
                                              equalTo("$.detail.value.message")))))),
                      atIndex(0));

              var document = JsonPath.parse(messages.get(0).body());
              var dataRef = (String) document.read(JsonPath.compile("$.detail.dataref"));
              s3ObjectArn.set(dataRef);
            });

    // use as marker object
    putS3Object("STOP", RequestBody.empty());

    var s3ObjectKey = s3ObjectArn.get().substring("arn:aws:s3:::test-bucket/".length());

    var s3Objects = listS3Objects().contents();
    log.info("received s3 object(s): {}", s3Objects);

    assertThat(s3Objects)
        .extracting(S3Object::key)
        // order is not ensured by LocalStack
        .containsExactlyInAnyOrder("START", s3ObjectKey, "STOP");

    assertThat(getS3Object(s3ObjectKey)).asString(UTF_8).isEqualTo("hello from kafka");
  }

  @Order(3)
  @Test
  public void connectorTaskIsRunning() throws IOException, InterruptedException {
    assertThat(connectorStateOfTask(0)).isEqualTo("RUNNING");
  }
}
