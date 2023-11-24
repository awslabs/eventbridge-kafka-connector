# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.


## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass (see details [below](#running-tests-in-a-local-environment-docker-compose))
4. Commit to your fork using clear commit messages. **Please follow [conventional commit](https://www.conventionalcommits.org/) guidelines.**
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).


### Running Tests in a local Environment (Docker Compose)

#### Unit Tests

To verify unit tests pass run:

```bash
mvn clean test
```

#### Formatting

The CI pipeline will verify that source code files are correctly formatted (using `com.spotify.fmt:fmt-maven-plugin:check`) and license headers are present (using )

To verify correct formatting of your code run:

```bash
mvn com.spotify.fmt:fmt-maven-plugin:check
mvn com.mycila:license-maven-plugin:check
```

> **Note**  
> When you run `mvn package` the formatter will auto-format files.

#### Packaging

To create the (shaded) JAR(s) run:

```bash
mvn clean package -Drevision=$(git describe --tags --always)
```

#### Integration Tests with Docker Compose

To execute the integration tests (requires [Docker](https://docker.com/)) run:

```bash
export KAFKA_VERSION=3.5.1
export COMPOSE_FILE=e2e/docker_compose.yaml
mvn clean verify -Drevision=$(git describe --tags --always)
```

> **Note**  
> If Docker cannot be used, alternative solutions, such as [`Finch`](https://github.com/runfinch/finch) or
> [`Podman`](https://podman.io/) can be tried (untested).

#### Running a local Kafka Connect Test Environment

Requirements:

- Maven
- Docker and `docker-compose` CLI
- `curl`
- A way to create AWS resources (unless LocalStack is used), e.g. using `aws` CLI

These steps assume you are familiar with EventBridge Event Buses, Rules, and Targets. Consult the EventBridge
[documentation](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-what-is.html) for instructions how to set up
those resources. 

If [Docker](https://docker.com/) and the `docker-compose` CLI is installed, a Kafka test environment can be created
locally e.g., to create the local environment:

```bash
# create the JAR which will be loaded into Kafka Connect (Docker Volume)
mvn clean package -Drevision=$(git describe --tags --always)

# move to the e2e directory
cd e2e
```

Skip the next step if you already have these credentials configured in your environment:

```bash
# set credentials used by the connector to send events to EventBridge (or assume a corresponding role)
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=ABCDEFGHIJKLMNOPQRST
export AWS_SECRET_ACCESS_KEY=ABCDEFGHIJKLMNOPQRST
export AWS_SESSION_TOKEN=ABCDEFGHIJKLMNOPQRST
```

The following command removes any previous resources and creates the local test environment:

```bash
docker-compose -f docker_compose.yaml down --remove-orphans -v && docker-compose -f docker_compose.yaml up
```

> **Note**  
> The Docker Compose environment includes [LocalStack](https://localstack.cloud/) to emulate AWS resources, such as SQS and EventBridge. If you want to use LocalStack use `test` for the access key id and secret environment variable and pass `--endpoint-url=http://localhost:4566` to your `aws` CLI commands.

Move on to next section when you see following lines:

```bash
e2e-connect-1     | [2023-07-12 09:07:29,262] INFO REST resources initialized; server is started and ready to handle requests (org.apache.kafka.connect.runtime.rest.RestServer:302)
e2e-connect-1     | [2023-07-12 09:07:29,263] INFO Kafka Connect started (org.apache.kafka.connect.runtime.Connect:57)
```

Change the JSON configuration example `connect-config.json` (uses LocalStack defaults) according to your
environment. In a separate terminal (within the `e2e` folder) deploy the connector:

```bash
curl -i --fail-with-body -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://$(docker-compose -f docker_compose.yaml port connect 8083)/connectors/ -d @connect-config.json
```

The output should look like:

```bash
HTTP/1.1 201 Created
Date: Fri, 24 Nov 2023 14:11:22 GMT
Location: http://0.0.0.0:32816/connectors/eventbridge-e2e
Content-Type: application/json
Content-Length: 688
Server: Jetty(9.4.52.v20230823)

{"name":"eventbridge-e2e","config":{"auto.offset.reset":"earliest","connector.class":"software.amazon.event.kafkaconnector.EventBridgeSinkConnector","topics":"eventbridge-e2e","aws.eventbridge.connector.id":"eventbridge-e2e-connector","aws.eventbridge.eventbus.arn":"arn:aws:events:us-east-1:000000000000:event-bus/eventbridge-e2e","aws.eventbridge.region":"us-east-1","aws.eventbridge.endpoint.uri":"http://localstack:4566","key.converter":"org.apache.kafka.connect.storage.StringConverter","value.converter":"org.apache.kafka.connect.json.JsonConverter","value.converter.schemas.enable":"false","name":"eventbridge-e2e"},"tasks":[{"connector":"eventbridge-e2e","task":0}],"type":"sink"}
```

Before proceeding, verify that the Docker Compose logs do not show any errors, such as `WARN`, `ERROR`, or `FATAL` log
entries. You should see logs like:

```bash
e2e-connect-1     | [2023-07-04 09:35:59,034] TRACE [eventbridge-e2e|task-0] EventBridgeSinkTask put called with 0 records: [] (software.amazon.event.kafkaconnector.EventBridgeSinkTask:68)
e2e-connect-1     | [2023-07-04 09:35:59,035] TRACE [eventbridge-e2e|task-0] Returning early: 0 records received (software.amazon.event.kafkaconnector.EventBridgeSinkTask:70)
```

Produce a Kafka record to invoke the EventBridge sink connector: 

```bash
# open a shell in the Kafka broker container
docker-compose -f docker_compose.yaml exec -w /opt/bitnami/kafka/bin kafka /bin/bash

# produce an event
# replace the topic with your connector settings if needed
echo -n '{"hello":"world"}' | ./kafka-console-producer.sh --bootstrap-server kafka:29092 --topic eventbridge-e2e
```

The Docker Compose logs output should look like:

```console
e2e-connect-1     | [2023-07-04 09:54:55,824] TRACE [eventbridge-e2e|task-0] EventBridgeSinkTask put called with 1 records: [SinkRecord{kafkaOffset=1, timestampType=CreateTime} ConnectRecord{topic='eventbridge-e2e', kafkaPartition=0, key=null, keySchema=Schema{STRING}, value={hello=world}, valueSchema=null, timestamp=1688464495808, headers=ConnectHeaders(headers=)}] (software.amazon.event.kafkaconnector.EventBridgeSinkTask:68)
e2e-connect-1     | [2023-07-04 09:54:55,824] TRACE [eventbridge-e2e|task-0] putItems call started: start=2023-07-04T09:54:55.824953210Z attempts=1 maxRetries=2 (software.amazon.event.kafkaconnector.EventBridgeSinkTask:81)
e2e-connect-1     | [2023-07-04 09:54:55,825] TRACE [eventbridge-e2e|task-0] Sending request to EventBridge: PutEventsRequest(Entries=[PutEventsRequestEntry(Source=kafka-connect.eventbridge-e2e-connector, Resources=[], DetailType=kafka-connect-eventbridge-e2e, Detail={"topic":"eventbridge-e2e","partition":0,"offset":0,"timestamp":1688464495808,"timestampType":"CreateTime","headers":[],"key":null,"value":{"hello":"world"}}, EventBusName=arn:aws:events:us-east-1:1234567890:event-bus/eventbridge-e2e)]) (software.amazon.event.kafkaconnector.EventBridgeWriter:152)
e2e-connect-1     | [2023-07-04 09:54:56,100] TRACE [eventbridge-e2e|task-0] putEvents response: [PutEventsResultEntry(EventId=29725ba0-c013-9457-0be2-51ba26708d3d)] (software.amazon.event.kafkaconnector.EventBridgeWriter:154)
e2e-connect-1     | [2023-07-04 09:54:56,301] TRACE [eventbridge-e2e|task-0] putItems call completed: start=2023-07-04T09:54:55.824953210Z completion=2023-07-04T09:54:56.301049627Z durationMillis=476 attempts=1 maxRetries=2 (software.amazon.event.kafkaconnector.EventBridgeSinkTask:105)
```

The output event should look similar to the below:

```json
{
    "version": "0",
    "id": "467efba4-6a53-d275-a52c-416a0c22abe4",
    "detail-type": "kafka-connect-eventbridge-e2e",
    "source": "kafka-connect.eventbridge-e2e-connector",
    "account": "1234567890",
    "time": "2023-07-06T16:02:19Z",
    "region": "us-east-1",
    "resources": [],
    "detail": {
        "topic": "eventbridge-e2e",
        "partition": 0,
        "offset": 0,
        "timestamp": 1688659338101,
        "timestampType": "CreateTime",
        "headers": [],
        "key": null,
        "value": {
            "hello": "world"
        }
    }
}
```

To tear down the environment run:

```bash
docker-compose -f docker_compose.yaml down --remove-orphans -v
```

## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' issues is a great place to start.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.
