/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static com.jayway.jsonpath.matchers.JsonPathMatchers.*;
import static java.net.http.HttpClient.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.CreateEventBusRequest;
import software.amazon.awssdk.services.eventbridge.model.PutRuleRequest;
import software.amazon.awssdk.services.eventbridge.model.PutTargetsRequest;
import software.amazon.awssdk.services.eventbridge.model.Target;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.ImmutableMap;

@TestInstance(PER_CLASS)
@Testcontainers
public class EventBridgeSinkConnectorIT {
  //  environment variables
  private static final String COMPOSE_FILE_ENV = "COMPOSE_FILE";
  private static final String KAFKA_VERSION_ENV = "KAFKA_VERSION";

  private static final String CONNECT_SERVICE = "connect_1";
  private static final int CONNECT_EXPOSED_SERVICE_PORT = 8083;

  private static final String LOCALSTACK_SERVICE = "localstack_1";
  private static final int LOCALSTACK_EXPOSED_SERVICE_PORT = 4566;

  // (@embano1): hardcoding ports here is a bit of a hack, but it's the easiest way to
  // allow direct invocation of docker-compose with fixed ports
  private static final String BOOTSTRAP_SERVER = "localhost:9092";
  private static final String RUNNING_STATE = "RUNNING";
  private static final String CONNECTOR_CONFIG_LOCATION = "e2e/connect-config.json";

  private static final String AWS_ACCESS_KEY_ID = "test";
  private static final String AWS_SECRET_ACCESS_KEY = "test";

  private static final String TEST_RESOURCE_NAME = "eventbridge-e2e";
  private static final String TEST_EVENT_KEY = "eventbridge-e2e";

  private static final Logger log = LoggerFactory.getLogger(EventBridgeSinkConnectorIT.class);

  private static File getComposeFile() {
    var filename = System.getenv(COMPOSE_FILE_ENV);
    if (filename == null || filename.trim().isEmpty()) {
      fail(COMPOSE_FILE_ENV + " environment variable must be set");
    }
    return new File(filename);
  }

  private static String getKafkaVersion() {
    var version = System.getenv(KAFKA_VERSION_ENV);
    if (version == null || version.trim().isEmpty()) {
      fail(KAFKA_VERSION_ENV + " environment variable must be set");
    }
    return version;
  }

  @Container
  private static final DockerComposeContainer<?> environment =
      new DockerComposeContainer<>("e2e", getComposeFile())
          .withLogConsumer("connect", new Slf4jLogConsumer(log).withSeparateOutputStreams())
          .withEnv("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID)
          .withEnv("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY)
          .withEnv(KAFKA_VERSION_ENV, getKafkaVersion())
          .withExposedService(
              CONNECT_SERVICE, CONNECT_EXPOSED_SERVICE_PORT, Wait.forHttp("/connectors"))
          .withExposedService(
              LOCALSTACK_SERVICE,
              LOCALSTACK_EXPOSED_SERVICE_PORT,
              Wait.forHttp("/_localstack/health"));

  private SqsClient sqsClient;
  private HttpClient httpClient;

  private URI getLocalstackEndpoint() {
    var port = environment.getServicePort(LOCALSTACK_SERVICE, LOCALSTACK_EXPOSED_SERVICE_PORT);
    return URI.create("http://localhost:" + port);
  }

  private URI buildKafkaConnectURI(String path) {
    var port = environment.getServicePort(CONNECT_SERVICE, CONNECT_EXPOSED_SERVICE_PORT);
    return URI.create("http://localhost:" + port + path);
  }

  @BeforeAll
  public void createAwsResources() {
    log.info("creating aws localstack resources");
    var credentials =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY));

    httpClient = newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    sqsClient =
        SqsClient.builder()
            .endpointOverride(getLocalstackEndpoint())
            .credentialsProvider(credentials)
            .region(Region.US_EAST_1)
            .build();

    var ebClient =
        EventBridgeClient.builder()
            .endpointOverride(getLocalstackEndpoint())
            .credentialsProvider(credentials)
            .region(Region.US_EAST_1)
            .build();

    var sqsResponse =
        sqsClient.createQueue(CreateQueueRequest.builder().queueName(TEST_RESOURCE_NAME).build());
    if (!sqsResponse.queueUrl().contains(TEST_RESOURCE_NAME)) {
      fail("failed to create queue: " + sqsResponse.sdkHttpResponse().toString());
    }

    var busResponse =
        ebClient.createEventBus(CreateEventBusRequest.builder().name(TEST_RESOURCE_NAME).build());
    if (!busResponse.eventBusArn().contains(TEST_RESOURCE_NAME)) {
      fail("failed to create event bus: " + busResponse.sdkHttpResponse().toString());
    }

    var ruleResponse =
        ebClient.putRule(
            PutRuleRequest.builder()
                .name(TEST_RESOURCE_NAME)
                .eventBusName(TEST_RESOURCE_NAME)
                .eventPattern("{\"source\":[{\"prefix\":\"kafka-connect\"}]}")
                .build());
    if (!ruleResponse.ruleArn().contains(TEST_RESOURCE_NAME)) {
      fail("failed to create rule: " + ruleResponse.sdkHttpResponse().toString());
    }

    var targets =
        PutTargetsRequest.builder()
            .eventBusName(TEST_RESOURCE_NAME)
            .rule(TEST_RESOURCE_NAME)
            .targets(Target.builder().id(TEST_RESOURCE_NAME).arn(getQueueArn()).build());
    var targetResponse = ebClient.putTargets(targets.build());
    if (!targetResponse.failedEntryCount().equals(0)) {
      fail("failed to create target: " + targetResponse.failedEntries().toString());
    }
  }

  @Test
  public void startConnector() throws IOException, InterruptedException {
    log.info("creating eventbridge sink connector");
    var request =
        HttpRequest.newBuilder()
            .uri(buildKafkaConnectURI("/connectors/"))
            .POST(HttpRequest.BodyPublishers.ofFile(Path.of(CONNECTOR_CONFIG_LOCATION)))
            .setHeader("Accept", "application/json")
            .setHeader("Content-Type", "application/json")
            .build();

    var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    log.info("kafka connect response {}", response);

    assertThat(response.statusCode())
        .withFailMessage(() -> "could not create connector")
        .isBetween(200, 299);

    await()
        .atMost(5, SECONDS)
        .pollInterval(1, SECONDS)
        .untilAsserted(
            () -> {
              log.info("waiting for eventbridge sink connector to enter {} state", RUNNING_STATE);
              var statusRequest =
                  HttpRequest.newBuilder()
                      .uri(buildKafkaConnectURI("/connectors/eventbridge-e2e/status"))
                      .setHeader("Accept", "application/json")
                      .build();

              var statusResponse =
                  httpClient.send(statusRequest, HttpResponse.BodyHandlers.ofString());
              assertThat(statusResponse.body()).contains(RUNNING_STATE);
            });
    log.info("eventbridge sink connector entered {} state", RUNNING_STATE);
  }

  @Test
  public void sendJsonRecordToKafkaReceiveFromSQS() {
    log.info("creating kafka producer");

    final Map<String, Object> producerConfig =
        ImmutableMap.of(
            BOOTSTRAP_SERVERS_CONFIG,
            BOOTSTRAP_SERVER,
            CLIENT_ID_CONFIG,
            UUID.randomUUID().toString(),
            ACKS_CONFIG,
            "all",
            RETRIES_CONFIG,
            2);

    var producer =
        new KafkaProducer<>(producerConfig, new StringSerializer(), new JsonSerializer());

    var mapper = new ObjectMapper();

    var jsonTestEvent =
        mapper
            .createObjectNode()
            .put("sentTimestamp", LocalDateTime.now().toString())
            .put("message", "hello from kafka");

    try {
      log.info("sending kafka json test record {} to topic {}", jsonTestEvent, TEST_RESOURCE_NAME);
      producer.send(new ProducerRecord<>(TEST_RESOURCE_NAME, TEST_EVENT_KEY, jsonTestEvent)).get();
      producer.flush();
      producer.close(Duration.ofSeconds(3));
    } catch (Exception e) {
      fail("could not send json test record", e);
    }
    log.info("successfully sent json test record to kafka");

    var receivedMessage = new AtomicReference<Message>(null);

    log.info("polling sqs queue {} for json test record", getQueueArn());
    await()
        .atMost(5, SECONDS)
        .pollInterval(1, SECONDS)
        .untilAsserted(
            () -> {
              var response =
                  sqsClient.receiveMessage(
                      ReceiveMessageRequest.builder()
                          .queueUrl(getQueueUrl())
                          .maxNumberOfMessages(1)
                          .build());

              var messages = response.messages();
              assertThat(messages).hasSize(1);

              receivedMessage.set(messages.get(0));
            });

    var messageBody = receivedMessage.get().body();
    log.info("received sqs message body: {}", messageBody);

    MatcherAssert.assertThat(
        messageBody,
        isJson(
            withJsonPath(
                "$.detail",
                allOf(
                    hasJsonPath("partition", greaterThanOrEqualTo(0)),
                    hasJsonPath("offset", greaterThanOrEqualTo(0)),
                    hasJsonPath("topic", equalTo(TEST_EVENT_KEY)),
                    hasJsonPath("key", equalTo(TEST_EVENT_KEY)),
                    hasJsonPath(
                        "value.sentTimestamp",
                        equalTo(jsonTestEvent.get("sentTimestamp").asText())),
                    hasJsonPath(
                        "value.message", equalTo(jsonTestEvent.get("message").asText()))))));
  }

  private String getQueueUrl() {
    var response =
        sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(TEST_RESOURCE_NAME).build());
    return response.queueUrl();
  }

  private String getQueueArn() {
    var response =
        sqsClient.getQueueAttributes(
            GetQueueAttributesRequest.builder()
                .queueUrl(getQueueUrl())
                .attributeNames(QueueAttributeName.QUEUE_ARN)
                .build());
    var queueArn = response.attributes().get(QueueAttributeName.QUEUE_ARN);
    if (queueArn == null) {
      fail("could not retrieve arn for queue " + TEST_RESOURCE_NAME);
    }
    return queueArn;
  }
}
