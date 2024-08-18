/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.auth;

import static software.amazon.awssdk.utils.StringUtils.isNotBlank;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.profiles.ProfileFileSupplier;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;

/** IAMUtility offers convenience functions for creating AWS IAM credential providers. */
public abstract class EventBridgeAwsCredentialsProviderFactory {

  private static final int stsRefreshDuration = 900; // min allowed value
  private static final Logger log =
      ContextAwareLoggerFactory.getLogger(EventBridgeAwsCredentialsProviderFactory.class);

  private EventBridgeAwsCredentialsProviderFactory() {
    // prevent instantiation
  }

  /**
   * Create an AWS credentials provider.
   *
   * <p>If a {@link AwsCredentialsProvider} implementing class name is provided, then an instance is
   * created by no-arg constructor. If the class also implements {@link Configurable}, then {@link
   * Configurable#configure(Map)} is called after instantiation.
   *
   * <p>If a role ARN is provided in the config, then an STS assume-role credentials provider is
   * created. The provider will automatically renew the assume-role session as needed.
   *
   * <p>If the role ARN is empty or null, then the default AWS credentials provider is returned.
   *
   * @param config Configuration containing optional {@link AwsCredentialsProvider} implementing
   *     class name, IAM role, session, etc.
   * @return AWS credentials provider
   */
  public static AwsCredentialsProvider getAwsCredentialsProvider(EventBridgeSinkConfig config) {
    if (isNotBlank(config.awsCredentialsProviderClass)) {
      try {
        // checks are already executed by EventBridgeSinkConnector#validate(Map)
        var clazz = Class.forName(config.awsCredentialsProviderClass);
        var ctor = clazz.getDeclaredConstructor();
        var obj = ctor.newInstance();
        if (Configurable.class.isAssignableFrom(clazz)) {
          ((Configurable) obj).configure(config.originals());
        }
        return (AwsCredentialsProvider) obj;
      } catch (final ClassNotFoundException
          | NoSuchMethodException
          | InvocationTargetException
          | InstantiationException
          | IllegalArgumentException
          | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    if (config.roleArn.trim().isBlank()) {
      log.info("Using aws default credentials provider");
      return getDefaultCredentialsProvider(config);
    }

    log.info(
        "Using aws sts credentials provider with roleArn={} sessionName={} externalID={} "
            + "stsRefreshDuration={}",
        config.roleArn,
        config.connectorId,
        config.externalId,
        stsRefreshDuration);
    return getStsAssumeRoleCredentialsProvider(config);
  }

  private static DefaultCredentialsProvider getDefaultCredentialsProvider(
      EventBridgeSinkConfig config) {
    var builder = DefaultCredentialsProvider.builder();

    // Leverage DefaultSupplier to automatically reload credentials on file refresh
    // https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-profiles.html#profile-reloading
    builder.profileFile(ProfileFileSupplier.defaultSupplier());

    var profileName = config.profileName;
    if (!profileName.isBlank()) {
      builder.profileName(profileName);
    }

    return builder.build();
  }

  private static StsAssumeRoleCredentialsProvider getStsAssumeRoleCredentialsProvider(
      EventBridgeSinkConfig config) {
    var stsClient = StsClient.builder().region(Region.of(config.region)).build();
    var requestBuilder =
        AssumeRoleRequest.builder()
            .roleArn(config.roleArn)
            .roleSessionName(config.connectorId)
            .durationSeconds(stsRefreshDuration);

    var externalID = config.externalId.trim();
    if (!externalID.isBlank()) {
      requestBuilder.roleSessionName(config.externalId);
    }

    var request = requestBuilder.build();
    return StsAssumeRoleCredentialsProvider.builder()
        .stsClient(stsClient)
        .refreshRequest(request)
        .build();
  }
}
