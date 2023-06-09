/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.auth;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

/** IAMUtility offers convenience functions for creating AWS IAM credential providers. */
public class EventBridgeCredentialsProvider {

  private static final int stsRefreshDuration = 900; // min allowed value
  private static final Logger log = LoggerFactory.getLogger(EventBridgeCredentialsProvider.class);

  /**
   * Create an IAM credentials provider.
   *
   * <p>If a role ARN is provided in the config, then an STS assume-role credentials provider is
   * created. The provider will automatically renew the assume-role session as needed.
   *
   * <p>If the role ARN is empty or null, then the default AWS credentials provider is returned.
   *
   * @param config Configuration containing optional IAM role, session, etc.
   * @return AWS credentials provider
   */
  public static AwsCredentialsProvider getCredentials(EventBridgeSinkConfig config) {
    if (config.roleArn.trim().isBlank()) {
      log.info("Using aws default credentials provider: role arn not set");
      return DefaultCredentialsProvider.create();
    }

    log.info(
        "Using aws sts credentials provider with roleArn={} sessionName={} externalID={} "
            + "stsRefreshDuration={}",
        config.roleArn,
        config.connectorId,
        config.externalId,
        stsRefreshDuration);
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
