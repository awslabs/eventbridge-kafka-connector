# Kafka Connector for Amazon EventBridge
[![Build](https://github.com/awslabs/eventbridge-kafka-connector/actions/workflows/build.yaml/badge.svg)](https://github.com/awslabs/eventbridge-kafka-connector/actions/workflows/build.yaml)
[![Latest Release](https://img.shields.io/github/release/awslabs/eventbridge-kafka-connector.svg?logo=github&style=flat-square)](https://github.com/awslabs/eventbridge-kafka-connector/releases/latest)


This Kafka *sink* connector for Amazon EventBridge allows you to send events (records) from one or multiple Kafka
topic(s) to the specified event bus, including useful features such as configurable topic to event `detail-type` name
mapping, IAM role-based authentication, support for [dead-letter
queues](https://kafka.apache.org/documentation/#sinkconnectorconfigs_errors.deadletterqueue.topic.name), and schema
registry support for Avro and Protocol Buffers (Protobuf). See [configuration](#configuration) below for details.

Amazon EventBridge event buses is a serverless event router that enables you to create scalable event-driven
applications by routing events between your own applications, third-party SaaS applications, and other AWS services. You
can set up routing rules to determine where to send your events, allowing for application architectures to react to
changes in your systems as they occur. To get started with Amazon EventBridge, visit our
[documentation](https://aws.amazon.com/eventbridge/resources/).

The connector is released as a community-supported open-source project with best effort support from the repository
maintainers.

## Installation

### Java Archive (JAR)

Two `kafka-eventbridge-sink` JAR files, are created on each
[release](https://github.com/awslabs/eventbridge-kafka-connector/releases). The JAR file `*-with-dependencies.jar`
contains all required dependencies of the connector, **excluding** Kafka Connect dependencies and (de)serializers, such
as `connect-api` and `connect-json`. To support additional (de)serializers, such as Avro and Protobuf using the [AWS
Glue Schema
Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-apache-kafka-connect),
install these dependencies in your Kafka Connect environment before deploying this connector.

### From Source

The following steps describe how to clone the repo and perform a clean packaging of the connector. Requires Maven and
Java Development Kit (JDK 11 or later).

```console
# clone repo
git clone https://github.com/awslabs/eventbridge-kafka-connector.git
cd eventbridge-kafka-connector

# create jar files
mvn clean package -Drevision=$(git describe --tags --always)
```

### From Source (Docker)

The following steps describe how to clone the repo and perform a clean packaging of the connector using
[Docker](https://docker.com/).

```console
# clone repo
git clone https://github.com/awslabs/eventbridge-kafka-connector.git
cd eventbridge-kafka-connector

# create jar files
docker run --rm -v maven-repo:/root/.m2 -v $(pwd):/src -w /src -it maven:3-eclipse-temurin-11 \
mvn clean package -Drevision=$(git describe --tags --always)
```

## Configuration

In addition to the common Kafka Connect [sink-related](https://kafka.apache.org/documentation.html#sinkconnectconfigs)
configuration options, this connector defines the following configuration properties.

| Property                             | Required | Default                    | Description                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------------ | -------- | -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `aws.eventbridge.connector.id`       | **Yes**  |                            | The unique ID of this connector (used in the EventBridge event `source` field as a suffix on `kafka-connect.` to uniquely identify a connector).                                                                                                                                                                                                                                           |
| `aws.eventbridge.region`             | **Yes**  |                            | The AWS region of the target event bus.                                                                                                                                                                                                                                                                                                                    |
| `aws.eventbridge.eventbus.arn`       | **Yes**  |                            | The ARN of the target event bus.                                                                                                                                                                                                                                                                                                                           |
| `aws.eventbridge.detail.types`       | No       | `"kafka-connect-${topic}"` | The `detail-type` that will be used for the EventBridge events. Can be defined per topic e.g., `"topic1:MyDetailType, topic2:MyDetailType"`, as a single expression with a dynamic `${topic}` placeholder for all topics e.g., `"my-detail-type-${topic}"` or as a static value without additional topic information for all topics e.g, `"my-detail-type"`. |
| `aws.eventbridge.eventbus.resources` | No       |                            | Optional [`Resources`](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEventsRequestEntry.html) (comma-seperated) to add to each EventBridge event.                                                                                                                                                                                     |
| `aws.eventbridge.endpoint.uri`       | No       |                            | An optional [service endpoint](https://docs.aws.amazon.com/general/latest/gr/ev.html) URI used to connect to EventBridge.                                                                                                                                                                                                                                  |
| `aws.eventbridge.retries.max`        | No       | `2`                        | The maximum number of retry attempts when sending events to EventBridge.                                                                                                                                                                                                                                                                                   |
| `aws.eventbridge.retries.delay`      | No       | `200`                      | The retry delay in milliseconds between each retry attempt.                                                                                                                                                                                                                                                                                                |
| `aws.eventbridge.iam.role.arn`       | No       |                            | Uses [STS](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) to assume the specified IAM role with periodic refresh. The connector ID is used as the session name.                                                                                                                                                                |
| `aws.eventbridge.iam.external.id`    | No       |                            | The IAM external id (optional) when role-based authentication is used.                                                                                                                                                                                                                                                                                     |
### Examples

#### JSON Encoding

The following minimal configuration configures the connector with default values, consuming Kafka records from the topic
`"json-values-topic"` with record keys as `String` and `JSON` values (without schema), and sending events to the custom
EventBridge event bus `"kafkabus"` in region `"us-east-1"`.

```json5
{
    "name": "EventBridgeSink-Json",
    "config": {
        // consume from earliest record or last checkpointed offset if available
        "auto.offset.reset": "earliest",
        "connector.class": "software.amazon.event.kafkaconnector.EventBridgeSinkConnector",
        "topics": "json-values-topic",
        "aws.eventbridge.connector.id": "my-json-values-connector",
        "aws.eventbridge.eventbus.arn": "arn:aws:events:us-east-1:1234567890:event-bus/kafkabus",
        "aws.eventbridge.region": "us-east-1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        // see note below on JSON schemas
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": false
    }
}
```

> **Note**  
> Currently, when using `JsonConverter` for keys or values, the connector uses a fixed configuration
> `schemas.enable=false`, i.e., JSON schemas are not included in the outgoing EventBridge event.


#### JSON Encoding with Dead-Letter Queue

Continuing the example above, the following configuration defines a dead-letter queue (DLQ), i.e., topic, `"json-dlq"`
which will be created with an replication factor of `1` if it does not exist. Records which cannot be converted or
delivered to EventBridge will be sent to this DLQ.

```json5
{
    "name": "EventBridgeSink-Json",
    "config": {
        "auto.offset.reset": "earliest",
        "connector.class": "software.amazon.event.kafkaconnector.EventBridgeSinkConnector",
        "topics": "json-values-topic",
        "aws.eventbridge.connector.id": "my-json-values-connector",
        "aws.eventbridge.eventbus.arn": "arn:aws:events:us-east-1:1234567890:event-bus/kafkabus",
        "aws.eventbridge.region": "us-east-1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": false
        "errors.tolerance":"all",
        "errors.deadletterqueue.topic.name":"json-dlq",
        "errors.deadletterqueue.topic.replication.factor":1
    }
}
```

#### Avro Encoding with multiple Topics, IAM Role and custom Retries

The following configuration shows some advanced options, such as multiple topics with customized `detail-type` mapping,
customized retry behavior, and IAM-based authentication, and how to deserialize Avro-encoded record values (with
JSON-encoded keys) using [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
(GSR).

```json5
{
  "name": "EventBridgeSink-Avro",
  "config": {
    "auto.offset.reset": "earliest",
    "connector.class": "software.amazon.event.kafkaconnector.EventBridgeSinkConnector",
    "topics": "avro-topic-1,avro-topic-2",
    "aws.eventbridge.connector.id": "avro-test-connector",
    "aws.eventbridge.eventbus.arn": "arn:aws:events:us-east-1:1234567890:event-bus/kafkabus",
    "aws.eventbridge.region": "us-east-1",
    // customized retries
    "aws.eventbridge.retries.max": 1,
    "aws.eventbridge.retries.delay": 1000,
    // custom detail-type mapping with topic suffix
    "aws.eventbridge.detail.types": "avro-test-${topic}",
    // IAM-based authentication
    "aws.eventbridge.iam.role.arn":"arn:aws:iam::1234567890:role/EventBridgePutEventsRole",
    "tasks.max": 1,
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.inventory",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    // dependencies (Classes) must be in the connector $CLASSPATH
    "value.converter": "com.amazonaws.services.schemaregistry.kafkaconnect.AWSKafkaAvroConverter",
    // GSR region
    "value.converter.region": "us-east-1",
    // GSR registry to use (expects schemas to exist and IAM role to have permission to read)
    "value.converter.registry.name": "avro-kafka-eventbridge",
    "value.converter.avroRecordType": "GENERIC_RECORD",
  }
}
```

> **Note**   
> This connector does not include custom (de)serializers, such as `AWSKafkaAvroConverter` as shown above. Refer to the
> Kafka Connect, schema registry (e.g.
> [GSR](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-apache-kafka-connect)),
> or (de)serializer (e.g. GSR [SerDes](https://github.com/awslabs/aws-glue-schema-registry)) documentation how to
> provide them to Kafka connectors.

### Topic to `detail-type` Mapping

The main task of this connector is to convert Kafka records to EventBridge events. Since this connector can be used to
consume from multiple Kafka topics, which an EventBridge user might want to filter later on, the mapping of topic names
to the EventBridge `detail-type`, i.e. event type, is customizable.

The default, i.e., when the configuration option `aws.eventbridge.detail.types` is not set, uses `kafka-connect-` as a
prefix, followed by the topic name of each individual record. Alternatively, a custom `detail-type` can be defined per
topic, provided as a comma-separated list with the syntax `"<topic_name>:<detail_type>,<topic_name>:<detail_type>,..."`
e.g., `"orders:com.example.org.orders.event.v0,customers:com.example.org.customers.event.v0"`. Records from the `orders`
topic would result in EventBridge events with a `detail-type: com.example.org.orders.event.v0`.

If only the topic name should be used, a single expression with a dynamic `${topic}` placeholder for all topics can be
used e.g., `"my-detail-type-${topic}"` (using a hardcoded prefix), `"${topic}"` (only topic name), or as a static value
without additional topic information `"my-detail-type"`.

### Retry Behavior

By default, the connector is configured to retry failed [`PutEvents`
API](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html) calls, i.e. an Exception was
thrown, `2` times, i.e., `3` total attempts, with a constant delay between each retry of `200` milliseconds. These
values can be configured (see [configuration](#configuration)). The following exceptions (incl. their subclasses) are
considered retryable: `AwsServiceException`, `SdkClientException`, `ExecutionException`, `InterruptedException`,
`TimeoutException`. 

> **Note**  
> `EventBridgeException`s with a `413` status code (`PutEventsRequestEntry` limit exceeded) are not retried.

> **Note**   
> The setting `aws.eventbridge.retries.max` is also used on the underlying AWS SDK client, which automatically handles
> certain retryable errors, such as throttling, without immediately throwing an exception. Currently, this can lead to
> more than the desired retry attempts since those exceptions are also considered retryable by the connector code.

### Authentication and Permissions

#### Authentication (IAM Credentials)

Each connector task creates an EventBridge client using either the AWS
[DefaultCredentialsProvider](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html)
to look up credentials e.g., from the environment, or the
[StsAssumeRoleCredentialsProvider](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/sts/auth/StsAssumeRoleCredentialsProvider.html)
if the configuration property `“aws.eventbridge.iam.role.arn”` is set. Alternatively, the [MSK Config
Providers](https://github.com/aws-samples/msk-config-providers) project can be used for additional authentication
options.

#### Permissions (IAM Policy)

The connector only requires `events:PutEvents` permission as shown in the IAM policy example below.

```json5
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowPutEventsKafkaConnector",
            "Effect": "Allow",
            "Action": "events:PutEvents",
            "Resource": "arn:aws:events:us-east-1:1234567890:event-bus/kafkabus"
        }
    ]
}
```

> **Note**  
> If you use the Glue Schema Registry, the IAM role needs additional permissions to retrieve schemas e.g.,
> using the managed policy `AWSGlueSchemaRegistryReadonlyAccess`. Please refer to the Glue Schema Registry
> [documentation](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).

## Deployment to Kafka Connect

The connector can be deployed like any Kafka connector e.g., using the Kafka Connect REST API:

```console
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://<kafka-connect-api>:<kafka-connect-port>/connectors/ -d @connector_config.json
```

> **Note**   
> On Amazon Managed Streaming for Apache Kafka (MSK), follow the official
> [documentation](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-getting-started.html) how to create
a custom plugin (connector).

### Example Event

Below is an example of an event received by an EventBridge target using the minimal JSON configuration described
[above](#json-encoding).

```json5
{
    // fields set by EventBridge
    "version": "0",
    "id": "dbc1c73a-c51d-0c0e-ca61-ab9278974c57",
    "account": "1234567890",
    "time": "2023-05-23T11:38:46Z",
    "region": "us-east-1",
    // customizable fields (see configuration)
    // detail-type is highly configurable (see section below)
    "detail-type": "kafka-connect-json-values-topic",
    // source is kafka-connect.<connector-id>
    "source": "kafka-connect.my-json-values-connector",
    "resources": [],
    // contains Kafka record key/value and metadata
    "detail": {
        "topic": "json-values-topic",
        "partition": 0,
        "offset": 0,
        "timestamp": 1684841916831,
        "timestampType": "CreateTime",
        "headers": [],
        "key": "order-1",
        "value": {
            "orderItems": [
                "item-1",
                "item-2"
            ],
            "orderCreatedTime": "Tue May 23 13:38:46 CEST 2023"
        }
    }
}
```

## Contributing and Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

## Credits

A HUGE **THANK YOU** to @flo-mair and @maschnetwork for their initial contributions to this project.