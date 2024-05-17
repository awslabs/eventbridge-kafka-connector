/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ACCESS_KEY_ID;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_SECRET_ACCESS_KEY;
import static software.amazon.awssdk.profiles.ProfileFileSystemSetting.AWS_PROFILE;
import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.*;

import java.util.HashMap;
import java.util.List;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class EventBridgeSinkConfigValidatorTest {

  @Test
  public void validID() {
    var configValue = new ConfigValue(AWS_CONNECTOR_ID_CONFIG);
    configValue.value("my-connector-id");

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void invalidEmptyID() {
    var configValue = new ConfigValue(AWS_CONNECTOR_ID_CONFIG);
    configValue.value("");

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void invalidNullID() {
    var configValue = new ConfigValue(AWS_CONNECTOR_ID_CONFIG);

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void validRegion() {
    var configValue = new ConfigValue(AWS_REGION_CONFIG);
    configValue.value("eu-central-1");

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void invalidRegionValidation() {
    var configValue = new ConfigValue(AWS_REGION_CONFIG);
    configValue.value("invalid-region-1");

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void validBusArn() {
    var configValue = new ConfigValue(AWS_EVENTBUS_ARN_CONFIG);
    configValue.value("arn:aws:events:eu-central-1:123456789101:event-bus/default");

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validBusArnGovCloud() {
    var configValue = new ConfigValue(AWS_EVENTBUS_ARN_CONFIG);
    configValue.value("arn:aws-us-gov:events:us-east-1:123456789101:event-bus/default");

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void invalidBusArnValidation() {
    var configValue = new ConfigValue(AWS_EVENTBUS_ARN_CONFIG);
    configValue.value("arn:aws:invalid:eu-stuff-1:156789101:event-bus");

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void invalidBusArnMissingNameValidation() {
    var configValue = new ConfigValue(AWS_EVENTBUS_ARN_CONFIG);
    configValue.value("arn:aws:events:eu-central-1:123456789101:event-bus/");

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void validRoleArn() {
    var configValue = new ConfigValue(AWS_ROLE_ARN_CONFIG);
    configValue.value("arn:aws:iam::123456789101:role/role-name-with-path");
    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validEmptyRoleArn() {
    var configValue = new ConfigValue(AWS_ROLE_ARN_CONFIG);
    configValue.value("");
    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void invalidRoleArn() {
    var configValue = new ConfigValue(AWS_ROLE_ARN_CONFIG);
    configValue.value("arn:aws:invalid:1234:role/role-name-with-path");
    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void invalidEndpointId0() {
    var configValue = new ConfigValue(AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG);
    configValue.value("abcendpoint");
    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void invalidEndpointId1() {
    var configValue = new ConfigValue(AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG);
    configValue.value("abc.def.xyz");
    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void invalidRetryValue0() {
    var configValue = new ConfigValue(AWS_RETRIES_CONFIG);
    configValue.value(-1);

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void invalidRetryValue11() {
    var configValue = new ConfigValue(AWS_RETRIES_CONFIG);
    configValue.value(11);

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void validRetry() {
    var configValue = new ConfigValue(AWS_RETRIES_CONFIG);
    configValue.value(5);

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validDetailTypeMultiple() {
    var configValue = new ConfigValue(AWS_DETAIL_TYPES_CONFIG);
    configValue.value(List.of("topic1:test", "topic2:something-else"));

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validDetailTypeSingleWithVariable() {
    var configValue = new ConfigValue(AWS_DETAIL_TYPES_CONFIG);
    configValue.value(List.of("kafka-connect-${topic}"));

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validEndpointIdNullValue() {
    var configValue = new ConfigValue(AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG);
    configValue.value(null);

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validEndpointIdEmptyString() {
    var configValue = new ConfigValue(AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG);
    configValue.value("");

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validEndpointId() {
    var configValue = new ConfigValue(AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG);
    configValue.value("abcde.veo");

    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void invalidProfileNameConfigWithAwsProfileEnvSet() {
    var configValue = new ConfigValue(AWS_PROFILE_NAME_CONFIG);
    configValue.value("testprofile");

    var envMock =
        new HashMap<String, String>() {
          {
            put(AWS_PROFILE.toString(), "testenvprofilevalue");
          }
        };

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue, envMock::get);
        });
  }

  @Test
  public void invalidProfileNameConfigWithAwsCredentialsEnvSet() {
    var configValue = new ConfigValue(AWS_PROFILE_NAME_CONFIG);
    configValue.value("testprofile");

    var envMock =
        new HashMap<String, String>() {
          {
            // omitting optional AWS_SESSION_TOKEN in this test
            put(AWS_ACCESS_KEY_ID.toString(), "testkey");
            put(AWS_SECRET_ACCESS_KEY.toString(), "testkey");
          }
        };

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue, envMock::get);
        });
  }

  @Test
  public void validProfileNameConfigWithNoEnvSet() {
    var configValue = new ConfigValue(AWS_PROFILE_NAME_CONFIG);
    configValue.value("testprofile");

    var envMock = new HashMap<String, String>();
    EventBridgeSinkConfigValidator.validate(configValue, envMock::get);
  }

  @Test
  public void validProfileNameEmptyStringWithAwsCredentials() {
    var configValue = new ConfigValue(AWS_PROFILE_NAME_CONFIG);
    configValue.value("");

    var envMock =
        new HashMap<String, String>() {
          {
            put(AWS_ACCESS_KEY_ID.toString(), "testkey");
            put(AWS_SECRET_ACCESS_KEY.toString(), "testkey");
          }
        };
    EventBridgeSinkConfigValidator.validate(configValue, envMock::get);
  }

  @Test
  public void invalidDetailTypes() {
    var configValue = new ConfigValue(AWS_DETAIL_TYPES_CONFIG);
    configValue.value(List.of("kafka-connect:test", "something", "else"));

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void invalidTopicDetailTypeMapperClass() {
    var configValue = new ConfigValue(AWS_DETAIL_TYPES_MAPPER_CLASS);
    configValue.value("com.xyz.completely.made.up");

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }

  @Test
  public void validTopicDetailTypeMapperClass() {
    var configValue = new ConfigValue(AWS_DETAIL_TYPES_MAPPER_CLASS);
    configValue.value("software.amazon.event.kafkaconnector.mapping.DefaultDetailTypeMapper");
    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validEmptyOffloadingDefaultS3Bucket() {
    var configValue = new ConfigValue(AWS_OFFLOADING_DEFAULT_S3_BUCKET);
    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validOffloadingDefaultS3Bucket() {
    var configValue = new ConfigValue(AWS_OFFLOADING_DEFAULT_S3_BUCKET);
    configValue.value("test");
    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s 3", "-s3", "s3-"})
  public void invalidOffloadingDefaultS3Bucket(String value) {
    var configValue = new ConfigValue(AWS_OFFLOADING_DEFAULT_S3_BUCKET);
    configValue.value(value);
    assertThrows(ConfigException.class, () -> EventBridgeSinkConfigValidator.validate(configValue));
  }

  @Test
  public void validEmptyOffloadingDefaultFieldRef() {
    var configValue = new ConfigValue(AWS_OFFLOADING_DEFAULT_FIELDREF);
    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @Test
  public void validOffloadingDefaultFieldRef() {
    var configValue = new ConfigValue(AWS_OFFLOADING_DEFAULT_FIELDREF);
    configValue.value("$.detail.value.test");
    EventBridgeSinkConfigValidator.validate(configValue);
  }

  @ParameterizedTest
  @ValueSource(strings = {"$", "$.detail", "$.detail.[*]"})
  public void invalidOffloadingDefaultFieldRef(String value) {
    var configValue = new ConfigValue(AWS_OFFLOADING_DEFAULT_FIELDREF);
    configValue.value(value);
    assertThrows(ConfigException.class, () -> EventBridgeSinkConfigValidator.validate(configValue));
  }
}
