# Kafka Connector for Amazon EventBridge
[![Build](https://github.com/awslabs/eventbridge-kafka-connector/actions/workflows/build.yaml/badge.svg)](https://github.com/awslabs/eventbridge-kafka-connector/actions/workflows/build.yaml)
[![Java E2E](https://github.com/awslabs/eventbridge-kafka-connector/actions/workflows/e2e.yaml/badge.svg)](https://github.com/awslabs/eventbridge-kafka-connector/actions/workflows/e2e.yaml)
[![Latest Release](https://img.shields.io/github/release/awslabs/eventbridge-kafka-connector.svg?logo=github&style=flat-square)](https://github.com/awslabs/eventbridge-kafka-connector/releases/latest)


This Kafka *sink* connector for Amazon EventBridge allows you to send events (records) from one or multiple Kafka
topic(s) to the specified event bus, including useful features such as:

- offloading large events to S3 (✨ new in `v1.3.0`)
- configurable topic to event `detail-type` name mapping with option to provide a custom class to customize event `detail-type` naming (✨ new in `v1.3.0`)
- custom IAM profiles per connector
- IAM role-based authentication
- provide custom credentials provider class (✨ new in `v1.3.3`)
- support for [dead-letter
queues](https://kafka.apache.org/documentation/#sinkconnectorconfigs_errors.deadletterqueue.topic.name)
- and schema registry support for Avro and Protocol Buffers (Protobuf). 

See [configuration](#configuration) below for details.

Amazon EventBridge Event Bus is a serverless event router that enables you to create scalable event-driven applications
by routing events between your own applications, third-party SaaS applications, and other AWS services. You can set up
routing rules to determine where to send your events, allowing for application architectures to react to changes in your
systems as they occur. To get started with Amazon EventBridge, visit our
[documentation](https://aws.amazon.com/eventbridge/resources/).

The connector is released as a community-supported open-source project with best effort support from the repository
maintainers.

## Installation

### Confluent Connector Hub

Download the connector from [Confluent Connector Hub](https://www.confluent.io/hub/aws/kafka-eventbridge-sink).

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

Clone the repo:

```console
git clone https://github.com/awslabs/eventbridge-kafka-connector.git
cd eventbridge-kafka-connector
```

Create JAR artifacts:

```console
mvn clean package -Drevision=$(git describe --tags --always)
```

### From Source (Docker)

The following steps describe how to clone the repo and perform a clean packaging of the connector using
[Docker](https://docker.com/).

Clone the repo:

```console
# clone repo
git clone https://github.com/awslabs/eventbridge-kafka-connector.git
cd eventbridge-kafka-connector
```

Create JAR artifacts:

```console
docker run --rm -v $(pwd):/src -w /src -it maven:3-eclipse-temurin-11 \
mvn clean package -Drevision=$(git describe --tags --always)
```

> [!TIP]
> If you want to reuse your local Maven cache and/or persist the Maven dependencies pulled, add  
> `-v <local_maven_folder>:/root/.m2 ` to the above command.

## Configuration

In addition to the common Kafka Connect [sink-related](https://kafka.apache.org/documentation.html#sinkconnectconfigs)
configuration options, this connector defines the following configuration properties.

| Property                                          | Required | Default                                                                                                                                                                                           | Description                                                                                                                                                                                                                                                                                                                                                  |
|---------------------------------------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `aws.eventbridge.connector.id`                    | **Yes**  |                                                                                                                                                                                                   | The unique ID of this connector (used in the EventBridge event `source` field as a suffix on `kafka-connect.` to uniquely identify a connector).                                                                                                                                                                                                             |
| `aws.eventbridge.region`                          | **Yes**  |                                                                                                                                                                                                   | The AWS region of the target event bus.                                                                                                                                                                                                                                                                                                                      |
| `aws.eventbridge.eventbus.arn`                    | **Yes**  |                                                                                                                                                                                                   | The ARN of the target event bus.                                                                                                                                                                                                                                                                                                                             |
| `aws.eventbridge.endpoint.uri`                    | No       |                                                                                                                                                                                                   | An optional [service endpoint](https://docs.aws.amazon.com/general/latest/gr/ev.html) URI used to connect to EventBridge.                                                                                                                                                                                                                                    |
| `aws.eventbridge.eventbus.global.endpoint.id`     | No       |                                                                                                                                                                                                   | An optional global endpoint ID of the target event bus specified using `abcde.xyz` syntax (see API [documentation](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-global-endpoints.html)).                                                                                                                                                      |
| `aws.eventbridge.eventbus.resources`              | No       |                                                                                                                                                                                                   | Optional [`Resources`](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEventsRequestEntry.html) (comma-seperated) to add to each EventBridge event.                                                                                                                                                                                       |
| `aws.eventbridge.detail.types`                    | No       | `"kafka-connect-${topic}"`                                                                                                                                                                        | The `detail-type` that will be used for the EventBridge events. Can be defined per topic e.g., `"topic1:MyDetailType, topic2:MyDetailType"`, as a single expression with a dynamic `${topic}` placeholder for all topics e.g., `"my-detail-type-${topic}"` or as a static value without additional topic information for all topics e.g, `"my-detail-type"`. |
| `aws.eventbridge.detail.types.mapper.class`       | No       |                                                                                                                                                                                                   | An optional class name implementing `software.amazon.event.kafkaconnector.mapping.DetailTypeMapper` to customize the EventBridge `detail-type` [field](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-events-structure.html) mapping. If specified, the configuration property `aws.eventbridge.detail.types` is ignored.                       |
| `aws.eventbridge.time.mapper.class`       | No       |                                                                                                                                                                                                   | An optional class name implementing `software.amazon.event.kafkaconnector.mapping.TimeMapper` to customize the EventBridge `Time` [field](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-events-structure.html) mapping. If not specified, the event `Time` is set by EventBridge.                                             |
| `aws.eventbridge.retries.max`                     | No       | `2`                                                                                                                                                                                               | The maximum number of retry attempts when sending events to EventBridge.                                                                                                                                                                                                                                                                                     |
| `aws.eventbridge.retries.delay`                   | No       | `200`                                                                                                                                                                                             | The retry delay in milliseconds between each retry attempt.                                                                                                                                                                                                                                                                                                  |
| `aws.eventbridge.auth.credentials_provider.class` | No       | `software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider` or `software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider` if `aws.eventbridge.iam.role.arn` is provided | An optional class name of the credentials provider to use. It must implement `software.amazon.awssdk.auth.credentials.AwsCredentialsProvider` with a no-arg constructor and optionally `org.apache.kafka.common.Configurable` to configure the provider after instantiation.                                                                                 |
| `aws.eventbridge.iam.profile.name`                | No       |                                                                                                                                                                                                   | Use the specified IAM profile to resolve credentials See [Using different Configuration Profiles per Connector](#using-different-configuration-profiles-per-connector) for details                                                                                                                                                                           |
| `aws.eventbridge.iam.role.arn`                    | No       |                                                                                                                                                                                                   | Uses [STS](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp.html) to assume the specified IAM role with periodic refresh. The connector ID is used as the session name.                                                                                                                                                                  |
| `aws.eventbridge.iam.external.id`                 | No       |                                                                                                                                                                                                   | The IAM external id (optional) when role-based authentication is used.                                                                                                                                                                                                                                                                                       |
| `aws.eventbridge.offloading.default.s3.bucket`    | No       |                                                                                                                                                                                                   | The S3 bucket to use to offload events to S3 (see [Offloading large events (payloads) to S3](#offloading-large-events-payloads-to-s3))                                                                                                                                                                                                                       |
| `aws.eventbridge.offloading.default.fieldref`     | No       | `$.detail.value`                                                                                                                                                                                  | The part of the event (payload) to offload to S3 (only active when `aws.eventbridge.offloading.default.s3.bucket` is set)                                                                                                                                                                                                                                    |

> [!NOTE]
> When using the default retry configuration (or retries > 0), the connector provides *at-least-once* delivery semantics
> for **valid** Kafka records, i.e., records which can be correctly (de)serialized before making a delivery attempt to
> EventBridge.

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

> [!NOTE]
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
        "value.converter.schemas.enable": false,
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
    "value.converter.avroRecordType": "GENERIC_RECORD"
  }
}
```

> [!IMPORTANT]
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

> [!TIP] 
> You can implement custom `detail-type` mapping by specifying a custom class in the
> `aws.eventbridge.detail.types.mapper.class` configuration property.

### Offloading large events (payloads) to S3

The current `PutEvents` size [limit](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html) in
EventBridge is 256KB. This can be problematic in cases where Kafka topics contain records exceeding this limit. By
default, the connector logs a warning when trying to send those events to EventBridge which can be ignored (dropped) or
sent to a Kafka dead-letter topic (see [Payloads exceeding PutEvents Limit](#payloads-exceeding-putevents-limit)).

Alternatively, the connector can be configured to offload (parts of) the event to S3 before calling the `PutEvents` API.
This is also known as the claim-check pattern. When enabled (see [Configuration](#configure-offloading)), every record
received from the associated Kafka topics in the connector which matches the
[JSONPath](https://www.ietf.org/archive/id/draft-goessner-dispatch-jsonpath-00.html) expression defined in
`aws.eventbridge.offloading.default.fieldref`(default: `$.detail.value`) will be offloaded.

#### Configure Offloading

To enable offloading, specify an S3 bucket via `aws.eventbridge.offloading.default.s3.bucket`.

> [!NOTE] 
> The IAM credentials/role used in the connector needs
> [`PutObject`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) permissions.

Unless overwritten by `aws.eventbridge.offloading.default.fieldref`, the connector will offload the value in
`$.detail.value` to S3, delete that key from the event and add claim-check information to the event metadata (see
examples below). The JSONPath expression applies to the converted EventBridge event before calling `PutEvents` to EventBridge. 

The benefits of this approach over other offloading implementations is flexibility in which parts of the events should
be offloaded and retaining as much of the original event as possible to harness the powerful event filtering
capabilities in EventBridge. For example, some events in a topic might contain large blobs of binary/base64-encoded data
which most consumers are not interested. In those cases, offloading helps to trim down event (payload) size and giving
the consumer(s) interested in the full payload the option to fully reconstruct the event based on the offloaded S3
object and metadata added to the event structure.

> [!NOTE] 
> Array and wildcard references are not allowed in the JSONPath expression defined in
> `aws.eventbridge.offloading.default.fieldref` and the JSONPath must always begin with `$.detail.value`.

#### Examples

Assuming offloading is enabled via the setting `aws.eventbridge.offloading.default.s3.bucket="my-offloading-bucket"` and
the following event structure which the S3 offloading logic in the connector operates on before making the final
`PutEvents` API call to EventBridge:

```json
{
    "version": "0",
    "id": "dbc1c73a-c51d-0c0e-ca61-ab9278974c57",
    "account": "1234567890",
    "time": "2023-05-23T11:38:46Z",
    "region": "us-east-1",
    "detail-type": "kafka-connect-json-values-topic",
    "source": "kafka-connect.my-json-values-connector",
    "resources": [],
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
            "orderCreatedTime": "Tue May 23 13:38:46 CEST 2023",
            "orderPreferences": null
        }
    }
}
```

If `aws.eventbridge.offloading.default.fieldref` is `$.detail.value` (the default), the resulting event sent to EventBridge would be:

```json
{
    "version": "0",
    "id": "dbc1c73a-c51d-0c0e-ca61-ab9278974c57",
    "account": "1234567890",
    "time": "2023-05-23T11:38:46Z",
    "region": "us-east-1",
    "detail-type": "kafka-connect-json-values-topic",
    "source": "kafka-connect.my-json-values-connector",
    "resources": [],
    "detail": {
        "topic": "json-values-topic",
        "partition": 0,
        "offset": 0,
        "timestamp": 1684841916831,
        "timestampType": "CreateTime",
        "headers": [],
        "key": "order-1",
        "dataref": "arn:aws:s3:::my-offloading-bucket/2d10c6f6-31e9-43b4-8706-51b4cf5534d8",
        "datarefJsonPath": "$.detail.value"
    }
}
```

In the S3 bucket `my-offloading-bucket` there would be an object `2d10c6f6-31e9-43b4-8706-51b4cf5534d8` containing:

```json
{
  "orderItems": [
      "item-1",
      "item-2"
  ],
  "orderCreatedTime": "Tue May 23 13:38:46 CEST 2023",
  "orderPreferences": null
}
```

Continuing the example, if `aws.eventbridge.offloading.default.fieldref` is `$.detail.value.non-existing-key`,
offloading would pass this event through without modification. The resulting event would be the same as the input event
without offloading information:

```json
{
    "version": "0",
    "id": "dbc1c73a-c51d-0c0e-ca61-ab9278974c57",
    "account": "1234567890",
    "time": "2023-05-23T11:38:46Z",
    "region": "us-east-1",
    "detail-type": "kafka-connect-json-values-topic",
    "source": "kafka-connect.my-json-values-connector",
    "resources": [],
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
            "orderCreatedTime": "Tue May 23 13:38:46 CEST 2023",
            "orderPreferences": null
        }
    }
}
```

If `aws.eventbridge.offloading.default.fieldref` is `$.detail.value.orderPreferences` and matches a key with a `null`
value, offloading is also skipped as there is nothing to offload. The resulting event would be the same as the input
event without offloading information:

```json
{
    "version": "0",
    "id": "dbc1c73a-c51d-0c0e-ca61-ab9278974c57",
    "account": "1234567890",
    "time": "2023-05-23T11:38:46Z",
    "region": "us-east-1",
    "detail-type": "kafka-connect-json-values-topic",
    "source": "kafka-connect.my-json-values-connector",
    "resources": [],
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
            "orderCreatedTime": "Tue May 23 13:38:46 CEST 2023",
            "orderPreferences": null
        }
    }
}
```

> [!NOTE]
> If offloading matches a key with an empty object `{}` or array `[]`, these values are considered a match and will be
> offloaded just as any other matched value.

### Retry Behavior

By default, the connector is configured to retry failed [`PutEvents`
API](https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html) calls, i.e. an Exception was
thrown, `2` times, i.e., `3` total attempts, with a constant delay between each retry of `200` milliseconds. These
values can be configured (see [configuration](#configuration)). The following exceptions (incl. their subclasses) are
considered retryable: `AwsServiceException`, `SdkClientException`, `ExecutionException`, `InterruptedException`,
`TimeoutException`. 

> [!NOTE]
> `EventBridgeException`s with a `413` status code (`PutEventsRequestEntry` limit exceeded) are not retried.

> [!NOTE] 
> The setting `aws.eventbridge.retries.max` is also used on the underlying AWS SDK client, which automatically handles
> certain retryable errors, such as throttling, without immediately throwing an exception. Currently, this can lead to
> more than the desired retry attempts since those exceptions are also considered retryable by the connector code.

### Authentication and Permissions

#### Authentication (IAM Credentials)

Each connector task creates an EventBridge client using the AWS
[DefaultCredentialsProvider](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html)
to look up credentials. AWS credential providers use a predefined configuration and configuration
[order](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html#credentials-default) to
retrieve credentials from the various credential [sources](https://docs.aws.amazon.com/sdkref/latest/guide/access.html).

For example, you can provide (temporary) credentials to the connector using `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` environment variables. For information how to use AWS `config` and
`credentials` profiles to resolve credentials, see [Using different Configuration Profiles per Connector](#using-different-configuration-profiles-per-connector).

When the configuration property `“aws.eventbridge.iam.role.arn”` is set, the
[StsAssumeRoleCredentialsProvider](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/sts/auth/StsAssumeRoleCredentialsProvider.html)
is directly used to assume the specified IAM role and periodically refresh credentials with STS. The STS client uses the
configured `region` of the connector for the STS client and retrieves credentials using the `DefaultCredentialsProvider`
retrieval chain described above.

#### Required Connector Permissions to send events to EventBridge (IAM Policy)

The connector only requires `events:PutEvents` permission as shown in the IAM policy example below. For details refer to
the "Managing access permissions to your Amazon EventBridge resources"
[documentation](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-manage-iam-access.html).

```json5
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowPutEventsKafkaConnector",
            "Effect": "Allow",
            "Action": "events:PutEvents",
            "Resource": "<ARN of your event bus>"
        }
    ]
}
```

> [!IMPORTANT]
> If you use the Glue Schema Registry, the IAM role needs additional permissions to retrieve schemas e.g.,
> using the managed policy `AWSGlueSchemaRegistryReadonlyAccess`. Please refer to the Glue Schema Registry
> [documentation](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).

#### Using different Configuration Profiles per Connector

If you run multiple EventBridge connectors in your Kafka Connect environment, using environment variables or Java system
properties to configure your connectors means that each connector will be configured **with the same IAM permissions**.
If you want to configure multiple connectors with specific (different) IAM profiles from your `config` and `credentials`
files, the connector configuration option `aws.eventbridge.iam.profile.name` can be used.

With the connector configuration option `aws.eventbridge.iam.profile.name` you specify which profile the specific
connector will use. 

> [!IMPORTANT]
> Environment variables, such as `AWS_PROFILE` or AWS access keys always take precedence over the configuration
> files and **must not** be set for this configuration option to take effect.

Steps to configure a connector with a configuration profile:

First, set `"aws.eventbridge.iam.profile.name": "my-custom-profile"` in the connector JSON configuration file (replace
example values with your desired profile name). Then, create (or mount) the AWS `config` and `credentials` files in your
Kafka Connect host(s). If the configuration files are not located/mounted in the [default
location](https://docs.aws.amazon.com/sdkref/latest/guide/file-location.html), set the environment variables
`AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE` accordingly. For example, with Docker you can mount them from your
local machine using Docker volume mounts and environment variables (see example below).

##### Docker Compose Example for a custom profile "my-custom-profile"

`config` file:

```ini
[profile my-custom-profile]
output=text # not used by the SDK, for illustration purposes
```

`credentials` file:

```ini
[my-custom-profile]
aws_access_key_id=AKIAIOSFODNN7EXAMPLE
aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
aws_session_token = IQoJb3JpZ2luX2IQoJb3JpZ2luX2IQoJb3JpZ2luX2IQoJb3JpZ2luX2IQoJb3JpZVERYLONGSTRINGEXAMPLE
```

Docker Compose file (snippet):

```yaml
  connect:
    # (snip)
    environment:
      AWS_CONFIG_FILE: '/aws/config'
      AWS_SHARED_CREDENTIALS_FILE: '/aws/credentials'
    volumes:
      - /Users/example/.aws:/aws # mount credentials from local host to /aws folder
```

You can also use role-based authentication with this approach by referencing a `source_profile` in the `config` file:

`config` file (role-based authentication):

```ini
[profile my-custom-profile]
role_arn = arn:aws:iam::0123456789:role/KafkaConnectorPutEvents
source_profile = default # assume role using credentials using from the default profile specified in the credentials file
```

#### Custom credentials provider

To use your own credentials provider, the class must implement the interface of [AwsCredentialsProvider](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/AwsCredentialsProvider.html) with a no-arg constructor and optionally the Kafka [Configurable](https://kafka.apache.org/38/javadoc/org/apache/kafka/common/Configurable.html) interface to configure the provider after instantiation.

Example configuration to use custom credentials provider `com.example.MyCustomCredentialsProvider`:

```json5
{
    "name": "EventBridgeSink-CustomCredentialsProvider",
    "config": {
        // other configuration attributes are omitted for clarity
        "aws.eventbridge.auth.credentials_provider.class": "com.example.MyCustomCredentialsProvider"
    }
}
```

> [!IMPORTANT]
> Since the class must be loadable from Kafka Connect, place the (uber) JAR with your custom credentials provider (and third-party dependencies) to a directory already listed in the plugin path (`plugin.path`).

## Deployment to Kafka Connect

The connector can be deployed like any Kafka connector e.g., using the Kafka Connect REST API:

```console
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://<kafka-connect-api>:<kafka-connect-port>/connectors/ -d @connector_config.json
```

> [!IMPORTANT] 
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

#### Example EventBridge Rule Pattern

The following Rule pattern would match the above event, i.e., any event where:

- `source` is **exactly** `kafka-connect.my-json-values-connector` and 
- `detail.key` **starts with** `order` and
- the field `orderItems` **exists** in the `details.value` object

```json5
{
  "source": ["kafka-connect.my-json-values-connector"],
  "detail": {
    "key": [{"prefix": "order"}],
    "value": {
      "orderItems": [{"exists": true}]
    }
  }
}
```

> [!TIP]
> Consult the EventBridge event patterns
> [documentation](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-event-patterns.html) for a complete
> explanation of available patterns.
## Troubleshooting

Common issues are around schema handling, authentication and authorization (IAM), and debugging the event flow.

### Schemas

If you see the following errors, check your connector configuration if it uses the correct key and value schema
settings.

**Error:**

The following error is caused when the `JsonConverter` is used and configured to use a schema within the Kafka record.
If the Kafka record was not produced with a JSON schema, i.e., only the JSON value, deserialization will fail with:

```console
org.apache.kafka.connect.errors.DataException: JsonConverter with schemas.enable requires "schema" and "payload" 
fields and may not contain additional fields. If you are trying to deserialize plain JSON data, 
set schemas.enable=false in your converter configuration.
```

**Resolution:**

```console
"value.converter": "org.apache.kafka.connect.json.JsonConverter",
"value.converter.schemas.enable": "false",
```

**Error:**

The following error is caused when an `AvroConverter` is used but the respective key/value is not Avro-encoded:

```console
org.apache.kafka.common.errors.SerializationException: Error deserializing Avro message for id -1
org.apache.kafka.common.errors.SerializationException: Unknown magic byte!
```

**Resolution:**

Change the key and/or value converters from Avro to the actual schema/payload type stored in the topic.

### IAM

When invalid IAM credentials are used, such as due to expired tokens or insufficient permissions, the connector will
throw an exception after an `PutEvents` API call attempt to EventBridge or during key/value deserialization when an
external schema registry with authentication is used. An example error message due to insufficient `PutEvents`
permissions looks like:

```console
org.apache.kafka.connect.errors.ConnectException: software.amazon.event.kafkaconnector.exceptions.EventBridgeWriterException: 
java.util.concurrent.ExecutionException: software.amazon.awssdk.services.eventbridge.model.EventBridgeException: 
User: arn:aws:sts::1234567890:assumed-role/some/role is not authorized to perform: events:PutEvents on resource: arn:aws:events:us-east-1:1234567890:event-bus/kafkabus because no identity-based policy allows the events:PutEvents action 
(Service: EventBridge, Status Code: 400, Request ID: e5ed0fb7-535d-4417-b38b-110f8495d0cb)
```

### Throttling (API Rate Limiting)

By default, the underlying AWS SDK client used will automatically handle throttle errors (exceptions) when the
`PutEvents` ingestion [quota](https://docs.aws.amazon.com/general/latest/gr/ev.html) for the account/region is exceeded.
However, depending on your quota and ingestion rate, if the client keeps hitting the rate limit it might throw an
exception to the connector. When setting `aws.eventbridge.retries.max` greater than `0`, the connector will attempt to
retry such a failed `PutEvents` attempt up to `aws.eventbridge.retries.max`. If `aws.eventbridge.retries.max` is 0 or
the retry budget is exhausted, a terminal `ConnectException` is thrown and the task will be stopped.

We recommend to verify your `PutEvents` account quota for the specific AWS
[region](https://docs.aws.amazon.com/general/latest/gr/ev.html) and adjusting the Kafka Connect sink setting
`consumer.override.max.poll.records` accordingly. For example, if your `PutEvents` quota is `500`, setting
`consumer.override.max.poll.records=400` leaves enough headroom.

> [!NOTE] 
> The EventBridge `PutEvents` quota is an account-level soft quota, i.e., it applies to the sum of all `PutEvents`
> requests in the same account, such as running multiple tasks of this connector. If you need to increase the quota
> beyond the hard limit, reach out to the EventBridge service team to better understand your use case and needs.

> [!NOTE] 
> `consumer.override.max.poll.interval.ms` is a related setting after which a consumer is considered failed and will
> leave the consumer group. Continuing the example above, if `consumer.override.max.poll.records=400` and
> `consumer.override.max.poll.interval.ms=300000` (the default as of Kafka 3.5), it means that processing `400` records
> is allowed to take up to 5 minutes, i.e., 750 milliseconds per record/event, before considering the consumer (task)
> failed.

### Payloads exceeding `PutEvents` Limit

EventBridge has a [limit](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevent-size.html) of 256KB on
the request size used in `PutEvents`. When a Kafka record exceeds this threshold, the connector will log a warning and
ignore (skip) over the record. Optionally, a dead-letter topic can be
[configured](#json-encoding-with-dead-letter-queue) where such records are sent to or [offloading to
S3](#configure-offloading) can be enabled.

```console
[2023-05-26 09:01:21,149] WARN [EventBridgeSink-Json|task-0] [@69c7029] Marking record as failed: code=413 message=EventBridge batch size limit exceeded topic=json-test partition=0 offset=0 (software.amazon.event.kafkaconnector.EventBridgeWriter:244)
[2023-05-26 09:01:21,149] WARN [EventBridgeSink-Json|task-0] [@69c7029] Dead-letter queue not configured: skipping failed record (software.amazon.event.kafkaconnector.EventBridgeSinkTask:147)
software.amazon.event.kafkaconnector.exceptions.EventBridgePartialFailureResponse: statusCode=413 errorMessage=EventBridge batch size limit exceeded topic=json-test partition=0 offset=0
```

### Debugging Event Flow (TRACE-level logging)

The connector will periodically (asynchronously) on a per-task basis report the count of successful `PutEvents` API
calls e.g.:

```console
[2023-05-25 11:53:04,598] INFO [EventBridgeSink-Json|task-0] Total records sent=15 (software.amazon.event.kafkaconnector.util.StatusReporter:36)
```

> [!TIP]
> Depending on your Kafka Connect environment, you can enable `[EventBridgeSink-Json|task-0]` logging style using this environment variable in Kafka Connect `CONNECT_LOG4J_APPENDER_STDOUT_LAYOUT_CONVERSIONPATTERN: "[%d] %p %X{connector.context}%m (%c:%L)%n"`

By enabling `TRACE`-level logging, the connector will emit additional log messages, such as the underlying AWS SDK
client configuration, records received from Kafka Connect, `PutEvents` stats, such as start, end time and duration, etc.

```console
[2023-05-25 12:01:56,882] TRACE [EventBridgeSink-Json|task-0] [@69c7029] EventBridgeSinkTask put called with 1 records: [SinkRecord{kafkaOffset=0, timestampType=CreateTime} ConnectRecord{topic='json-test', kafkaPartition=0, key=my-key, keySchema=Schema{STRING}, value={sentTime=Thu May 25 14:01:56 CEST 2023}, valueSchema=null, timestamp=1685016116860, headers=ConnectHeaders(headers=)}] (software.amazon.event.kafkaconnector.EventBridgeSinkTask:57)
[2023-05-25 12:01:56,889] TRACE [EventBridgeSink-Json|task-0] [@69c7029] EventBridgeSinkTask putItems call started: start=2023-05-25T12:01:56.889640Z attempts=1 maxRetries=0 (software.amazon.event.kafkaconnector.EventBridgeSinkTask:77)
[2023-05-25 12:01:56,909] TRACE [EventBridgeSink-Json|task-0] [@69c7029] EventBridgeWriter sending request to eventbridge: PutEventsRequest(Entries=[PutEventsRequestEntry(Source=kafka-connect.json-test-connector, Resources=[], DetailType=kafka-connect-json-test, Detail={"topic":"json-test","partition":0,"offset":0,"timestamp":1685016116860,"timestampType":"CreateTime","headers":[],"key":"my-key","value":{"sentTime":"Thu May 25 14:01:56 CEST 2023"}}, EventBusName=arn:aws:events:us-east-1:1234567890:event-bus/kafkabus)]) (software.amazon.event.kafkaconnector.EventBridgeWriter:140)
[2023-05-25 12:01:57,242] TRACE [EventBridgeSink-Json|task-0] [@69c7029] EventBridgeWriter putEvents response: [PutEventsResultEntry(EventId=875b7f21-f098-8b55-ea7a-a4d235079bfb)] (software.amazon.event.kafkaconnector.EventBridgeWriter:142)
[2023-05-25 12:01:57,242] TRACE [EventBridgeSink-Json|task-0] [@69c7029] EventBridgeSinkTask putItems call completed: start=2023-05-25T12:01:56.889640Z completion=2023-05-25T12:01:57.242428Z durationMillis=352 attempts=1 maxRetries=0 (software.amazon.event.kafkaconnector.EventBridgeSinkTask:99)
```

Depending on your Kafka Connect environment, you can enable `TRACE`-level logging via environment variables on Kafka
Connect using `CONNECT_LOG4J_LOGGERS: "software.amazon.event.kafkaconnector=TRACE"`. Please consult your Kafka Connect
documentation how to configure and change log levels for a particular connector.

> **Warning**  
> Enabling `TRACE`-level logging can expose sensitive information due to logging record keys and values. It is strongly
> recommended to audit changes to the log level to guard against leaking sensitive data, such as personally identifiable
> information (PII).

## Contributing and Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

## Credits

A HUGE **THANK YOU** to @flo-mair and @maschnetwork for their initial contributions to this project.