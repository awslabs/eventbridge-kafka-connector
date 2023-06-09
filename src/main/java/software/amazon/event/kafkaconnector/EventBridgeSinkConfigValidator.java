/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.*;

import java.util.List;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.RegionMetadata;

public class EventBridgeSinkConfigValidator {

  private static final Logger log = LoggerFactory.getLogger(EventBridgeSinkConfigValidator.class);

  public static void validate(Config config) {
    config.configValues().stream()
        .filter(configValue -> configValue.value() != null)
        .forEach(EventBridgeSinkConfigValidator::validate);
  }

  public static void validate(ConfigValue configValue) {
    switch (configValue.name()) {
      case AWS_CONNECTOR_ID_CONFIG:
        {
          validateConnectorId(configValue);
          break;
        }
      case AWS_ENDPOINT_URI_CONFIG:
        {
          validateURI(configValue);
          break;
        }
      case AWS_EVENTBUS_ARN_CONFIG:
        {
          validateBusArn(configValue);
          break;
        }
      case AWS_RETRIES_CONFIG:
        {
          validateEventBusRetries(configValue);
          break;
        }
      case AWS_ROLE_ARN_CONFIG:
        {
          validateRoleArn(configValue);
          break;
        }
      case AWS_REGION_CONFIG:
        {
          validateRegion(configValue);
          break;
        }
      case AWS_DETAIL_TYPES_CONFIG:
        {
          validateDetailType(configValue);
          break;
        }
    }
  }

  private static void validateConnectorId(ConfigValue configValue) {
    var connectorId = (String) configValue.value();
    if (connectorId == null || connectorId.trim().isBlank()) {
      throw new ConfigException(configValue.name() + " must be set");
    }
  }

  private static void validateURI(ConfigValue configValue) {
    // TODO: validate optional URI here or when constructing client in task?
  }

  private static void validateBusArn(ConfigValue configValue) {
    var awsEventBusArn = (String) configValue.value();
    // example: arn:aws[-partition]:events:region:account:event-bus/bus-name
    var arnPattern = Pattern.compile("^arn:aws.*:events:\\w+(?:-\\w+)+:\\d{12}:event-bus/.+");
    validateValueWithPattern(configValue.name(), awsEventBusArn, arnPattern);
  }

  private static void validateEventBusRetries(ConfigValue configValue) {
    var eventbusRetries = (Integer) configValue.value();
    if (eventbusRetries < 0 | eventbusRetries > 10) {
      throw new ConfigException(configValue.name() + " cannot be less than 0 or greater than 10.");
    }
  }

  private static void validateRoleArn(ConfigValue configValue) {
    var roleArn = (String) configValue.value();
    // optional parameter
    if (roleArn == null || roleArn.trim().isBlank()) {
      return;
    }

    // scheme: arn:aws[-parition]:iam::account:role/role-name-with-path
    var arnPattern = Pattern.compile("^arn:aws:iam::\\d{12}:role/.+");
    validateValueWithPattern(configValue.name(), roleArn, arnPattern);
  }

  private static void validateRegion(ConfigValue configValue) {
    var awsRegion = (String) configValue.value();
    if (RegionMetadata.of(Region.of(awsRegion)) == null) {
      throw new ConfigException(
          awsRegion
              + " is not a supported region. "
              + "Please check the name and/or update to the latest AWS Java SDK.");
    }
  }

  private static void validateDetailType(ConfigValue configValue) {
    var detailTypes = (List<?>) configValue.value();
    if (detailTypes.size() > 1) {
      detailTypes.forEach(
          detailType -> {
            var topicToDetailTypePattern = Pattern.compile("^\\S+:\\S+$");
            validateValueWithPattern(
                configValue.name(), (String) detailType, topicToDetailTypePattern);
          });
    }
  }

  private static void validateValueWithPattern(String key, String value, Pattern pattern) {
    if (!pattern.matcher(value).find()) {
      throw new ConfigException(
          String.format(
              "key \"%s\" with value \"%s\" does not satisfy regular expression check: %s",
              key, value, pattern));
    }
  }
}
