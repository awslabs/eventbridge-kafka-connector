/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.offloading.S3EventBridgeEventDetailValueOffloading;

public class EventBridgeSinkConfig extends AbstractConfig {

  private static final Logger log =
      ContextAwareLoggerFactory.getLogger(EventBridgeSinkConfig.class);

  // used in event source and IAM session role name
  static final String AWS_CONNECTOR_ID_CONFIG = "aws.eventbridge.connector.id";
  static final String AWS_REGION_CONFIG = "aws.eventbridge.region";
  static final String AWS_ENDPOINT_URI_CONFIG = "aws.eventbridge.endpoint.uri";
  static final String AWS_ENDPOINT_URI_DOC =
      "An optional service endpoint URI used to connect to EventBridge.";
  static final String AWS_EVENTBUS_ARN_CONFIG = "aws.eventbridge.eventbus.arn";
  static final String AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG =
      "aws.eventbridge.eventbus.global.endpoint.id";
  static final String AWS_RETRIES_CONFIG = "aws.eventbridge.retries.max";
  static final String AWS_RETRIES_DELAY_CONFIG = "aws.eventbridge.retries.delay";
  static final String AWS_PROFILE_NAME_CONFIG = "aws.eventbridge.iam.profile.name";
  static final String AWS_ROLE_ARN_CONFIG = "aws.eventbridge.iam.role.arn";
  static final String AWS_ROLE_EXTERNAL_ID_CONFIG = "aws.eventbridge.iam.external.id";
  static final String AWS_DETAIL_TYPES_CONFIG = "aws.eventbridge.detail.types";
  static final String AWS_DETAIL_TYPES_MAPPER_CLASS = "aws.eventbridge.detail.types.mapper.class";
  static final String AWS_EVENTBUS_RESOURCES_CONFIG = "aws.eventbridge.eventbus.resources";
  static final String AWS_OFFLOADING_S3_DEFAULT_BUCKET =
      "aws.eventbridge.offloading.s3.default.bucket";
  static final String AWS_OFFLOADING_DEFAULT_FIELDREF =
      "aws.eventbridge.offloading.default.fieldref";

  private static final String AWS_CONNECTOR_ID_DOC =
      "The unique ID of this connector (used in the event source field to uniquely identify a connector).";
  private static final String AWS_REGION_DOC = "The AWS region of the event bus.";
  private static final String AWS_EVENTBUS_ARN_DOC = "The ARN of the target event bus.";
  private static final String AWS_EVENTBUS_ENDPOINT_ID_DOC =
      "An optional global endpoint ID of the target event bus.";
  private static final int AWS_RETRIES_DEFAULT = 2;
  private static final String AWS_RETRIES_DOC =
      "The maximum number of retry attempts when sending events to EventBridge.";
  private static final int AWS_RETRIES_DELAY_DEFAULT = 200; // 200ms
  private static final String AWS_RETRIES_DELAY_DOC =
      "The retry delay in milliseconds between each retry attempt.";
  private static final String AWS_ROLE_ARN_DOC =
      "An optional IAM role to authenticate and send events to EventBridge. "
          + "If not specified, AWS default credentials provider is used";
  private static final String AWS_ROLE_EXTERNAL_ID_CONFIG_DOC =
      "The IAM external id (optional) when role-based authentication is used";
  private static final String AWS_PROFILE_NAME_CONFIG_DOC =
      "The profile to use from the configuration and credentials files to retrieve IAM credentials";
  public static final String AWS_DETAIL_TYPES_DEFAULT = "kafka-connect-${topic}";
  public static final String AWS_OFFLOADING_S3_DEFAULT_BUCKET_DOC =
      "The S3 bucket to offload matched record value by JSON Path";
  public static final String AWS_OFFLOADING_DEFAULT_FIELDREF_DOC =
      "The JSON Path to offload record value";
  public static final String AWS_OFFLOADING_DEFAULT_FIELDREF_DEFAULT =
      S3EventBridgeEventDetailValueOffloading.JSON_PATH_PREFIX;

  private static final String AWS_DETAIL_TYPES_MAPPER_CLASS_DEFAULT =
      "software.amazon.event.kafkaconnector.mapping.DefaultDetailTypeMapper";
  private static final String AWS_DETAIL_TYPES_DOC =
      "The detail-type that will be used for the EventBridge events. "
          + "Can be defined per topic e.g., 'topic1:MyDetailType, topic2:MyDetailType', as a single expression "
          + "with a dynamic '${topic}' placeholder for all topics e.g., 'my-detail-type-${topic}', "
          + "or as a static value without additional topic information for all topics e.g., 'my-detail-type'.";

  private static final String AWS_DETAIL_TYPES_MAPPER_DOC =
      "Define a custom implementation class for the DetailTypeMapper interface to customize the mapping of Kafka topics or records to the EventBridge detail-type. Define full class path e.g. software.amazon.event.kafkaconnector.mapping.DefaultDetailTypeMapper.";

  private static final String AWS_EVENTBUS_RESOURCES_DOC =
      "An optional comma-separated list of strings to add to "
          + "the resources field in the outgoing EventBridge events.";

  public static final ConfigDef CONFIG_DEF = createConfigDef();
  public final String connectorId;
  public final String region;
  public final String eventBusArn;
  public final String endpointID;
  public final String endpointURI;
  public final String roleArn;
  public final String externalId;
  public final String profileName;
  public final List<String> resources;
  public final int maxRetries;
  public final long retriesDelay;
  public Map<String, String> detailTypeByTopic;
  public String detailType;
  public String detailTypeMapperClass;
  public String offloadingS3defaultBucket;
  public String offloadingDefaultFieldRef;

  public EventBridgeSinkConfig(final Map<?, ?> originalProps) {
    super(CONFIG_DEF, originalProps);
    this.connectorId = getString(AWS_CONNECTOR_ID_CONFIG);
    this.region = getString(AWS_REGION_CONFIG);
    this.eventBusArn = getString(AWS_EVENTBUS_ARN_CONFIG);
    this.endpointID = getString(AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG);
    this.endpointURI = getString(AWS_ENDPOINT_URI_CONFIG);
    this.roleArn = getString(AWS_ROLE_ARN_CONFIG);
    this.externalId = getString(AWS_ROLE_EXTERNAL_ID_CONFIG);
    this.profileName = getString(AWS_PROFILE_NAME_CONFIG);
    this.maxRetries = getInt(AWS_RETRIES_CONFIG);
    this.retriesDelay = getInt(AWS_RETRIES_DELAY_CONFIG);
    this.resources = getList(AWS_EVENTBUS_RESOURCES_CONFIG);
    this.detailTypeMapperClass = getString(AWS_DETAIL_TYPES_MAPPER_CLASS);
    this.offloadingS3defaultBucket = getString(AWS_OFFLOADING_S3_DEFAULT_BUCKET);
    this.offloadingDefaultFieldRef = getString(AWS_OFFLOADING_DEFAULT_FIELDREF);

    var detailTypes = getList(AWS_DETAIL_TYPES_CONFIG);
    if (detailTypes.size() > 1 || detailTypes.get(0).contains(":")) {
      detailTypeByTopic =
          detailTypes.stream()
              .map(item -> item.split(":"))
              .collect(Collectors.toMap(topic -> topic[0], type -> type[1]));
    } else {
      detailType = detailTypes.get(0);
    }
    log.info(
        "EventBridge properties: connectorId={} eventBusArn={} eventBusRegion={} eventBusEndpointURI={} "
            + "eventBusMaxRetries={} eventBusRetriesDelay={} eventBusResources={} "
            + "eventBusEndpointID={} roleArn={} roleSessionName={} roleExternalID={}"
            + "offloadingS3defaultBucket={} offloadingDefaultFieldRef={}",
        connectorId,
        eventBusArn,
        region,
        endpointURI,
        maxRetries,
        retriesDelay,
        resources,
        endpointID,
        roleArn,
        connectorId,
        externalId,
        offloadingS3defaultBucket,
        offloadingDefaultFieldRef);
  }

  private static ConfigDef createConfigDef() {
    var configDef = new ConfigDef();
    addParams(configDef);
    return configDef;
  }

  private static void addParams(final ConfigDef configDef) {
    configDef.define(AWS_CONNECTOR_ID_CONFIG, Type.STRING, Importance.HIGH, AWS_CONNECTOR_ID_DOC);
    configDef.define(AWS_REGION_CONFIG, Type.STRING, Importance.HIGH, AWS_REGION_DOC);
    configDef.define(AWS_EVENTBUS_ARN_CONFIG, Type.STRING, Importance.HIGH, AWS_EVENTBUS_ARN_DOC);
    configDef.define(
        AWS_ENDPOINT_URI_CONFIG, Type.STRING, "", Importance.MEDIUM, AWS_ENDPOINT_URI_DOC);
    configDef.define(
        AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG,
        Type.STRING,
        "",
        Importance.MEDIUM,
        AWS_EVENTBUS_ENDPOINT_ID_DOC);
    configDef.define(AWS_ROLE_ARN_CONFIG, Type.STRING, "", Importance.MEDIUM, AWS_ROLE_ARN_DOC);
    configDef.define(
        AWS_ROLE_EXTERNAL_ID_CONFIG,
        Type.STRING,
        "",
        Importance.MEDIUM,
        AWS_ROLE_EXTERNAL_ID_CONFIG_DOC);
    configDef.define(
        AWS_PROFILE_NAME_CONFIG, Type.STRING, "", Importance.MEDIUM, AWS_PROFILE_NAME_CONFIG_DOC);
    configDef.define(
        AWS_RETRIES_CONFIG, Type.INT, AWS_RETRIES_DEFAULT, Importance.MEDIUM, AWS_RETRIES_DOC);
    configDef.define(
        AWS_RETRIES_DELAY_CONFIG,
        Type.INT,
        AWS_RETRIES_DELAY_DEFAULT,
        Importance.MEDIUM,
        AWS_RETRIES_DELAY_DOC);
    configDef.define(
        AWS_DETAIL_TYPES_CONFIG,
        Type.LIST,
        AWS_DETAIL_TYPES_DEFAULT,
        Importance.MEDIUM,
        AWS_DETAIL_TYPES_DOC);
    configDef.define(
        AWS_EVENTBUS_RESOURCES_CONFIG,
        Type.LIST,
        "",
        Importance.MEDIUM,
        AWS_EVENTBUS_RESOURCES_DOC);
    configDef.define(
        AWS_DETAIL_TYPES_MAPPER_CLASS,
        Type.STRING,
        AWS_DETAIL_TYPES_MAPPER_CLASS_DEFAULT,
        Importance.MEDIUM,
        AWS_DETAIL_TYPES_MAPPER_DOC);
    configDef.define(
        AWS_OFFLOADING_S3_DEFAULT_BUCKET,
        Type.STRING,
        "",
        Importance.MEDIUM,
        AWS_OFFLOADING_S3_DEFAULT_BUCKET_DOC);
    configDef.define(
        AWS_OFFLOADING_DEFAULT_FIELDREF,
        Type.STRING,
        AWS_OFFLOADING_DEFAULT_FIELDREF_DEFAULT,
        Importance.MEDIUM,
        AWS_OFFLOADING_DEFAULT_FIELDREF_DOC);
  }
}
