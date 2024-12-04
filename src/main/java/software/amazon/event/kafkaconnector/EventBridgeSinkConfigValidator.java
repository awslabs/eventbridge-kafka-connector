/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static software.amazon.awssdk.core.SdkSystemSetting.AWS_ACCESS_KEY_ID;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_SECRET_ACCESS_KEY;
import static software.amazon.awssdk.core.SdkSystemSetting.AWS_SESSION_TOKEN;
import static software.amazon.awssdk.profiles.ProfileFileSystemSetting.AWS_PROFILE;
import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.*;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.RegionMetadata;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.event.kafkaconnector.offloading.S3EventBridgeEventDetailValueOffloading;

public class EventBridgeSinkConfigValidator {

  @FunctionalInterface
  // for unit testing to mock environment variables
  interface EnvVarGetter {
    String get(String name);
  }

  public static void validate(Config config) {
    config.configValues().stream()
        .filter(configValue -> configValue.value() != null)
        .forEach(EventBridgeSinkConfigValidator::validate);
  }

  public static void validate(ConfigValue configValue) {
    validate(configValue, System::getenv);
  }

  public static void validate(ConfigValue configValue, EnvVarGetter getenv) {
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
      case AWS_CREDENTIAL_PROVIDER_CLASS:
        {
          validateAwsCredentialProviderClass(configValue);
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
      case AWS_EVENTBUS_GLOBAL_ENDPOINT_ID_CONFIG:
        {
          validateEndpointId(configValue);
          break;
        }

      case AWS_PROFILE_NAME_CONFIG:
        {
          validateProfileName(configValue, getenv);
          break;
        }

      case AWS_DETAIL_TYPES_MAPPER_CLASS:
        {
          validateClassExists(configValue);
          break;
        }

      case AWS_TIME_MAPPER_CLASS:
        {
          validateClassExists(configValue);
          break;
        }

      case AWS_OFFLOADING_DEFAULT_S3_ENDPOINT_URI:
        {
          validateURI(configValue);
          break;
        }

      case AWS_OFFLOADING_DEFAULT_S3_BUCKET:
        {
          nonStrictValidateOffloadingDefaultS3Bucket(configValue);
          break;
        }

      case AWS_OFFLOADING_DEFAULT_FIELDREF:
        {
          validateOffloadingDefaultFieldRef(configValue);
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

  private static void validateClassExists(ConfigValue configValue) {
    var mapperClass = (String) configValue.value();
    try {
      Class.forName(mapperClass);
    } catch (ClassNotFoundException e) {
      throw new ConfigException(
          mapperClass + " can't be loaded. Ensure the class path you have specified is correct.");
    }
  }

  private static void validateURI(ConfigValue configValue) {
    // TODO: validate optional URI here or when constructing client in task?
  }

  private static void validateAwsCredentialProviderClass(ConfigValue configValue) {
    var requiredInterface = AwsCredentialsProvider.class;
    var className = (String) configValue.value();
    if (StringUtils.isNotBlank(className)) {
      try {
        var clazz = Class.forName((String) configValue.value());
        if (!requiredInterface.isAssignableFrom(clazz)) {
          throw new ConfigException(
              "Class '"
                  + className
                  + "' does not implement '"
                  + requiredInterface.getCanonicalName()
                  + "'.");
        }
        clazz.getDeclaredConstructor();
      } catch (ClassNotFoundException e) {
        throw new ConfigException(
            "Class '"
                + className
                + "' can't be loaded. Ensure the class path you have specified is correct.");
      } catch (NoSuchMethodException e) {
        throw new ConfigException("Class '" + className + "' requires a no-arg constructor.");
      }
    }
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

  private static void validateEndpointId(ConfigValue configValue) {
    var endpointId = (String) configValue.value();
    // optional parameter
    if (endpointId == null || endpointId.trim().isBlank()) {
      return;
    }

    // https://docs.aws.amazon.com/eventbridge/latest/APIReference/API_PutEvents.html#API_PutEvents_RequestSyntax
    var arnPattern = Pattern.compile("^[A-Za-z0-9\\-]+[\\.][A-Za-z0-9\\-]+$");
    validateValueWithPattern(configValue.name(), endpointId, arnPattern);
  }

  private static void validateProfileName(ConfigValue configValue, EnvVarGetter envVarGetter) {
    var profileName = (String) configValue.value();
    // optional parameter
    if (profileName == null || profileName.trim().isBlank()) {
      return;
    }

    // throw if this config parameter and any AWS environment variables which overwrite its behavior
    // (from DefaultCredentialsProvider chain) are set
    var conflictingAwsEnvVars =
        Stream.of(AWS_PROFILE, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_ACCESS_KEY_ID)
            .map(Enum::toString)
            .filter(env -> (envVarGetter.get(env) != null))
            .collect(Collectors.toList());

    if (!conflictingAwsEnvVars.isEmpty()) {
      throw new ConfigException(
          String.format(
              "\"%s\" environment variable(s) are set. "
                  + "Unset the environment variable for \"%s\" to take effect.",
              conflictingAwsEnvVars, AWS_PROFILE_NAME_CONFIG));
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

  private static void nonStrictValidateOffloadingDefaultS3Bucket(ConfigValue configValue) {
    var value = (String) configValue.value();
    if (value == null || value.isBlank()) return;

    // https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
    var sufficient = Pattern.compile("^[a-z0-9][a-z0-9.-]{1,61}?[a-z0-9]$");
    if (!sufficient.matcher(value).find()) {
      throw new ConfigException(String.format("\"%s\" is not a valid S3 bucket name", value));
    }
  }

  private static void validateOffloadingDefaultFieldRef(ConfigValue configValue) {
    var value = (String) configValue.value();
    if (value == null || value.isBlank()) return;
    try {
      S3EventBridgeEventDetailValueOffloading.validateJsonPath(value);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(String.format("\"%s\" is not a valid offload JSON Path", value), e);
    }
  }
}
