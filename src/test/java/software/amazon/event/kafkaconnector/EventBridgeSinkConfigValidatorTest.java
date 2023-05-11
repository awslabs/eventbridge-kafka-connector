/*
 * // Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * // SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.event.kafkaconnector;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.*;

import java.util.List;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

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
  public void invalidConfiguration() {
    var configValue = new ConfigValue(AWS_DETAIL_TYPES_CONFIG);
    configValue.value(List.of("kafka-connect:test", "something", "else"));

    assertThrows(
        ConfigException.class,
        () -> {
          EventBridgeSinkConfigValidator.validate(configValue);
        });
  }
}
