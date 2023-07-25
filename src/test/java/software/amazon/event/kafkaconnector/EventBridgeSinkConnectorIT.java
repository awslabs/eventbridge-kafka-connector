/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.shaded.org.awaitility.Awaitility;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class EventBridgeSinkConnectorIT {
  //  environment variables
  private static final String COMPOSE_FILE_ENV = "COMPOSE_FILE";
  private static final String KAFKA_VERSION_ENV = "KAFKA_VERSION";

  // (@embano1): hardcoding ports here is a bit of a hack, but it's the easiest way to
  // allow direct invocation of docker-compose with fixed ports
  private static final URI LOCALSTACK_ENDPOINT = URI.create("http://localhost:4566");
  private static final String BOOTSTRAP_SERVER = "localhost:9092";
  private static final String RUNNING_STATE = "RUNNING";
  private static final String CONNECTOR_CONFIG_LOCATION = "e2e/connect-config.json";

  private static final String TEST_RESOURCE_NAME = "eventbridge-e2e";
  private static final String TEST_EVENT_KEY = "eventbridge-e2e";

  private final Logger log = LoggerFactory.getLogger(EventBridgeSinkConnectorIT.class);

  private DockerComposeContainer environment;
  private SqsClient sqsClient;
  private String KafkaVersion;
  private File ComposeFile;
  private HttpClient HttpClient;

  @BeforeAll
  public void setup() {
    KafkaVersion = System.getenv(KAFKA_VERSION_ENV);
    if (KafkaVersion == null || KafkaVersion.isEmpty()) {
      fail(KAFKA_VERSION_ENV + " environment variable must be set");
    }

    var ComposeFileLocation = System.getenv(COMPOSE_FILE_ENV);
    if (ComposeFileLocation == null || ComposeFileLocation.isEmpty()) {
      fail(COMPOSE_FILE_ENV + " environment variable must be set");
    }

    ComposeFile = new File(ComposeFileLocation);
    HttpClient =
        java.net.http.HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

    startDockerComposeEnvironment();
    createAwsResources();
  }

  @AfterAll
  public void cleanup() {
    environment.stop();
  }

  private void startDockerComposeEnvironment() {
    log.info(
        "starting docker compose environment: kafka_version={} docker_compose_file={}",
        KafkaVersion,
        ComposeFile.toString());
    try {
      environment =
          new DockerComposeContainer("e2e", ComposeFile)
              .withLogConsumer("connect", new Slf4jLogConsumer(log).withSeparateOutputStreams())
              .withEnv("AWS_ACCESS_KEY_ID", "test")
              .withEnv("AWS_SECRET_ACCESS_KEY", "test")
              .withEnv(KAFKA_VERSION_ENV, KafkaVersion)
              .withExposedService("connect_1", 8083)
              .withExposedService("localstack_1", 4566)
              .waitingFor("connect_1", new HttpWaitStrategy().forPort(8083))
              .waitingFor("localstack_1", new HttpWaitStrategy().forPort(4566));
      environment.start();
    } catch (Exception e) {
      fail("failed to run docker compose environment: " + e.getMessage());
    }
  }

  private void createAwsResources() {
    log.info("creating aws localstack resources");
    var credentials = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

    sqsClient =
        SqsClient.builder()
            .endpointOverride(LOCALSTACK_ENDPOINT)
            .credentialsProvider(credentials)
            .region(Region.US_EAST_1)
            .build();

    var ebClient =
        EventBridgeClient.builder()
            .endpointOverride(LOCALSTACK_ENDPOINT)
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
  public void startConnector() {
    log.info("creating eventbridge sink connector");
    try {
      var request =
          HttpRequest.newBuilder()
              .uri(new URI("http://localhost:8083/connectors/"))
              .POST(HttpRequest.BodyPublishers.ofFile(Path.of(CONNECTOR_CONFIG_LOCATION)))
              .setHeader("Accept", "application/json")
              .setHeader("Content-Type", "application/json")
              .build();

      var response = HttpClient.send(request, HttpResponse.BodyHandlers.ofString());
      log.info("kafka connect response {}", response);

      var statusCode = response.statusCode();
      Assertions.assertTrue(statusCode >= 200 && statusCode <= 299);
    } catch (Exception e) {
      fail("could not create connector", e);
    }

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              log.info("waiting for eventbridge sink connector to enter {} state", RUNNING_STATE);
              var statusRequest =
                  HttpRequest.newBuilder()
                      .uri(new URI("http://localhost:8083/connectors/eventbridge-e2e/status"))
                      .setHeader("Accept", "application/json")
                      .build();

              var response = HttpClient.send(statusRequest, HttpResponse.BodyHandlers.ofString());
              return response.body().contains(RUNNING_STATE);
            });
    log.info("eventbridge sink connector entered {} state", RUNNING_STATE);
  }

  @Test
  public void sendJsonRecordToKafkaReceiveFromSQS() {
    log.info("creating kafka producer");
    var producerConfig =
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            BOOTSTRAP_SERVER,
            ProducerConfig.CLIENT_ID_CONFIG,
            UUID.randomUUID().toString(),
            ProducerConfig.ACKS_CONFIG,
            "all",
            ProducerConfig.RETRIES_CONFIG,
            2);
    KafkaProducer producer =
        new KafkaProducer(producerConfig, new StringSerializer(), new JsonSerializer());

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
      producer.close(Duration.of(3, ChronoUnit.SECONDS));
    } catch (Exception e) {
      fail("could not send json test record", e);
    }
    log.info("successfully sent json test record to kafka");

    var gotMessageDetailValue = new AtomicReference<JsonNode>(mapper.createObjectNode());

    log.info("polling sqs queue {} for json test record", getQueueArn());
    Awaitility.await()
        .atMost(5, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              var receiveMessageResponse =
                  sqsClient.receiveMessage(
                      ReceiveMessageRequest.builder()
                          .queueUrl(getQueueUrl())
                          .maxNumberOfMessages(1)
                          .build());

              List<Message> messages = receiveMessageResponse.messages();
              Assertions.assertEquals(1, messages.size());

              var messageBody = messages.get(0).body();
              log.info("received sqs message body: {}", messageBody);

              var detailValue = new ObjectMapper().readTree(messageBody).path("detail");
              log.info("retrieved eventbridge event detail value: {}", detailValue);
              gotMessageDetailValue.set(detailValue);
            });

    Assertions.assertTrue(gotMessageDetailValue.get().path("partition").asInt(-1) >= 0);
    Assertions.assertTrue(gotMessageDetailValue.get().path("offset").asInt(-1) >= 0);
    Assertions.assertEquals(TEST_EVENT_KEY, gotMessageDetailValue.get().path("topic").asText(""));
    Assertions.assertEquals(TEST_EVENT_KEY, gotMessageDetailValue.get().path("key").asText(""));
    Assertions.assertEquals(jsonTestEvent, gotMessageDetailValue.get().path("value"));
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
