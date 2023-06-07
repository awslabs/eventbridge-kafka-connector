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

> **Note**  
> If you want to reuse your local Maven cache and/or persist the Maven dependencies pulled, add  
> `-v <local_maven_folder>:/root/.m2 ` to the above command.

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

> **Note**  
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

> **Note**  
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

### Payloads exceeding `PutEvents` Limit

EventBridge has a [limit](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevent-size.html) of 256KB on
the request size used in `PutEvents`. When a Kafka record exceeds this threshold, the connector will log a warning and
ignore (skip) over the record. Optionally, a dead-letter topic can be
[configured](#json-encoding-with-dead-letter-queue) where such records are sent to.

```console
[2023-05-26 09:01:21,149] WARN [EventBridgeSink-Json|task-0] Marking record as failed: code=413 message=EventBridge batch size limit exceeded topic=json-test partition=0 offset=0 (software.amazon.event.kafkaconnector.EventBridgeWriter:244)
[2023-05-26 09:01:21,149] WARN [EventBridgeSink-Json|task-0] Dead-letter queue not configured: skipping failed record (software.amazon.event.kafkaconnector.EventBridgeSinkTask:147)
software.amazon.event.kafkaconnector.exceptions.EventBridgePartialFailureResponse: statusCode=413 errorMessage=EventBridge batch size limit exceeded topic=json-test partition=0 offset=0
```

### Debugging Event Flow (TRACE-level logging)

The connector will periodically (asynchronously) on a per-task basis report the count of successful `PutEvents` API
calls e.g.:

```console
[2023-05-25 11:53:04,598] INFO [EventBridgeSink-Json|task-0] Total records sent=15 (software.amazon.event.kafkaconnector.util.StatusReporter:36)
```

> **Note**  
> Depending on your Kafka Connect environment, you can enable `[EventBridgeSink-Json|task-0]` logging style using this environment variable in Kafka Connect `CONNECT_LOG4J_APPENDER_STDOUT_LAYOUT_CONVERSIONPATTERN: "[%d] %p %X{connector.context}%m (%c:%L)%n"`

By enabling `TRACE`-level logging, the connector will emit additional log messages, such as the underlying AWS SDK
client configuration, records received from Kafka Connect, `PutEvents` stats, such as start, end time and duration, etc.

```console
[2023-05-25 12:01:56,882] TRACE [EventBridgeSink-Json|task-0] EventBridgeSinkTask put called with 1 records: [SinkRecord{kafkaOffset=0, timestampType=CreateTime} ConnectRecord{topic='json-test', kafkaPartition=0, key=my-key, keySchema=Schema{STRING}, value={sentTime=Thu May 25 14:01:56 CEST 2023}, valueSchema=null, timestamp=1685016116860, headers=ConnectHeaders(headers=)}] (software.amazon.event.kafkaconnector.EventBridgeSinkTask:57)
[2023-05-25 12:01:56,889] TRACE [EventBridgeSink-Json|task-0] EventBridgeSinkTask putItems call started: start=2023-05-25T12:01:56.889640Z attempts=1 maxRetries=0 (software.amazon.event.kafkaconnector.EventBridgeSinkTask:77)
[2023-05-25 12:01:56,909] TRACE [EventBridgeSink-Json|task-0] EventBridgeWriter sending request to eventbridge: PutEventsRequest(Entries=[PutEventsRequestEntry(Source=kafka-connect.json-test-connector, Resources=[], DetailType=kafka-connect-json-test, Detail={"topic":"json-test","partition":0,"offset":0,"timestamp":1685016116860,"timestampType":"CreateTime","headers":[],"key":"my-key","value":{"sentTime":"Thu May 25 14:01:56 CEST 2023"}}, EventBusName=arn:aws:events:us-east-1:1234567890:event-bus/kafkabus)]) (software.amazon.event.kafkaconnector.EventBridgeWriter:140)
[2023-05-25 12:01:57,242] TRACE [EventBridgeSink-Json|task-0] EventBridgeWriter putEvents response: [PutEventsResultEntry(EventId=875b7f21-f098-8b55-ea7a-a4d235079bfb)] (software.amazon.event.kafkaconnector.EventBridgeWriter:142)
[2023-05-25 12:01:57,242] TRACE [EventBridgeSink-Json|task-0] EventBridgeSinkTask putItems call completed: start=2023-05-25T12:01:56.889640Z completion=2023-05-25T12:01:57.242428Z durationMillis=352 attempts=1 maxRetries=0 (software.amazon.event.kafkaconnector.EventBridgeSinkTask:99)
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