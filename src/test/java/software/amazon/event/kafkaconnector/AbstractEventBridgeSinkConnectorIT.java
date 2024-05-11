/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.net.http.HttpClient.newBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;
import static software.amazon.awssdk.services.sqs.model.QueueAttributeName.QUEUE_ARN;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.utils.ImmutableMap;

@TestInstance(PER_CLASS)
@Testcontainers
public abstract class AbstractEventBridgeSinkConnectorIT {

  private static final String BOOTSTRAP_SERVER = "localhost:9092";

  //  environment variables
  private static final String COMPOSE_FILE_ENV = "COMPOSE_FILE";
  private static final String KAFKA_VERSION_ENV = "KAFKA_VERSION";

  private static final String CONNECT_SERVICE = "connect_1";
  private static final int CONNECT_EXPOSED_SERVICE_PORT = 8083;

  private static final String LOCALSTACK_SERVICE = "localstack_1";
  private static final int LOCALSTACK_EXPOSED_SERVICE_PORT = 4566;

  private static final String AWS_ACCESS_KEY_ID = "test";
  private static final String AWS_SECRET_ACCESS_KEY = "test";

  private final String RUNNING_STATE = "RUNNING";

  protected final String TEST_RESOURCE_NAME = "eventbridge-e2e";

  protected final String TEST_BUCKET_NAME = "test-bucket";

  protected static final Logger log =
      LoggerFactory.getLogger(AbstractEventBridgeSinkConnectorIT.class);

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

  protected HttpClient httpClient = newBuilder().connectTimeout(Duration.ofSeconds(2)).build();

  protected S3Client s3client;

  protected SqsClient sqsClient;

  private KafkaProducer<String, JsonNode> kafkaProducer =
      new KafkaProducer<>(
          ImmutableMap.of(
              BOOTSTRAP_SERVERS_CONFIG,
              BOOTSTRAP_SERVER,
              CLIENT_ID_CONFIG,
              UUID.randomUUID().toString(),
              ACKS_CONFIG,
              "all",
              RETRIES_CONFIG,
              2),
          new StringSerializer(),
          new JsonSerializer());

  @BeforeAll
  public void createAwsResources() {
    log.info("creating aws localstack resources");
    var credentials =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY));

    s3client =
        S3Client.builder()
            .endpointOverride(getLocalstackEndpoint())
            .forcePathStyle(true)
            .credentialsProvider(credentials)
            .region(Region.US_EAST_1)
            .build();

    var createBucketRequest = CreateBucketRequest.builder().bucket(TEST_BUCKET_NAME).build();

    var s3response = s3client.createBucket(createBucketRequest);
    if (!s3response.sdkHttpResponse().isSuccessful()) {
      fail("failed to create bucket: " + s3response.sdkHttpResponse());
    }

    sqsClient =
        SqsClient.builder()
            .endpointOverride(getLocalstackEndpoint())
            .credentialsProvider(credentials)
            .region(Region.US_EAST_1)
            .build();

    try (var client =
        EventBridgeClient.builder()
            .endpointOverride(getLocalstackEndpoint())
            .credentialsProvider(credentials)
            .region(Region.US_EAST_1)
            .build()) {

      var sqsResponse =
          sqsClient.createQueue(CreateQueueRequest.builder().queueName(TEST_RESOURCE_NAME).build());
      if (!sqsResponse.queueUrl().contains(TEST_RESOURCE_NAME)) {
        fail("failed to create queue: " + sqsResponse.sdkHttpResponse());
      }

      var busResponse =
          client.createEventBus(CreateEventBusRequest.builder().name(TEST_RESOURCE_NAME).build());
      if (!busResponse.eventBusArn().contains(TEST_RESOURCE_NAME)) {
        fail("failed to create event bus: " + busResponse.sdkHttpResponse());
      }

      var ruleResponse =
          client.putRule(
              PutRuleRequest.builder()
                  .name(TEST_RESOURCE_NAME)
                  .eventBusName(TEST_RESOURCE_NAME)
                  .eventPattern("{\"source\":[{\"prefix\":\"kafka-connect\"}]}")
                  .build());
      if (!ruleResponse.ruleArn().contains(TEST_RESOURCE_NAME)) {
        fail("failed to create rule: " + ruleResponse.sdkHttpResponse());
      }

      var targets =
          PutTargetsRequest.builder()
              .eventBusName(TEST_RESOURCE_NAME)
              .rule(TEST_RESOURCE_NAME)
              .targets(Target.builder().id(TEST_RESOURCE_NAME).arn(getQueueArn()).build());
      var targetResponse = client.putTargets(targets.build());
      if (!targetResponse.failedEntryCount().equals(0)) {
        fail("failed to create target: " + targetResponse.failedEntries());
      }
    }
  }

  protected void startConnector(final Path connectorConfig, final String connectorName)
      throws IOException, InterruptedException {
    log.info(
        "creating eventbridge sink connector with connector configuration from {}",
        connectorConfig);
    var request =
        HttpRequest.newBuilder()
            .uri(buildKafkaConnectURI("/connectors/"))
            .POST(HttpRequest.BodyPublishers.ofFile(connectorConfig))
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
                      .uri(buildKafkaConnectURI("/connectors/" + connectorName + "/status"))
                      .setHeader("Accept", "application/json")
                      .build();

              var statusResponse =
                  httpClient.send(statusRequest, HttpResponse.BodyHandlers.ofString());
              assertThat(statusResponse.body()).contains(RUNNING_STATE);
            });
    log.info("eventbridge sink connector entered {} state", RUNNING_STATE);
  }

  protected void sendKafkaRecord(ProducerRecord<String, JsonNode> record) {
    try {
      kafkaProducer.send(record).get();
      kafkaProducer.flush();
      kafkaProducer.close(Duration.ofSeconds(3));
    } catch (Exception e) {
      fail("could not send json test record", e);
    }
    log.info("successfully sent json test record to kafka");
  }

  protected ReceiveMessageResponse receiveSqsMessages(int maxNumberOfMessages) {
    return sqsClient.receiveMessage(
        ReceiveMessageRequest.builder()
            .queueUrl(getQueueUrl())
            .maxNumberOfMessages(maxNumberOfMessages)
            .build());
  }

  protected void putS3Object(String key, RequestBody requestBody) {
    s3client.putObject(
        PutObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(key).build(), requestBody);
  }

  protected ListObjectsResponse listS3Objects() {
    return s3client.listObjects(ListObjectsRequest.builder().bucket(TEST_BUCKET_NAME).build());
  }

  protected ResponseInputStream<GetObjectResponse> getS3Object(String key) {
    return s3client.getObject(GetObjectRequest.builder().bucket(TEST_BUCKET_NAME).key(key).build());
  }

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

  private URI getLocalstackEndpoint() {
    var port = environment.getServicePort(LOCALSTACK_SERVICE, LOCALSTACK_EXPOSED_SERVICE_PORT);
    return URI.create("http://localhost:" + port);
  }

  private URI buildKafkaConnectURI(String path) {
    var port = environment.getServicePort(CONNECT_SERVICE, CONNECT_EXPOSED_SERVICE_PORT);
    return URI.create("http://localhost:" + port + path);
  }

  private String getQueueUrl() {
    var response =
        sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(TEST_RESOURCE_NAME).build());
    return response.queueUrl();
  }

  protected String getQueueArn() {
    var response =
        sqsClient.getQueueAttributes(
            GetQueueAttributesRequest.builder()
                .queueUrl(getQueueUrl())
                .attributeNames(QUEUE_ARN)
                .build());
    var queueArn = response.attributes().get(QUEUE_ARN);
    if (queueArn == null) {
      fail("could not retrieve arn for queue " + TEST_RESOURCE_NAME);
    }
    return queueArn;
  }
}
