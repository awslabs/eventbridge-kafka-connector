/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.panic;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.reportOnly;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.retry;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeAsyncClient;
import software.amazon.awssdk.services.eventbridge.model.EventBridgeException;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.event.kafkaconnector.auth.EventBridgeCredentialsProvider;
import software.amazon.event.kafkaconnector.batch.DefaultEventBridgeBatching;
import software.amazon.event.kafkaconnector.batch.EventBridgeBatchingStrategy;
import software.amazon.event.kafkaconnector.mapping.DefaultEventBridgeMapper;
import software.amazon.event.kafkaconnector.mapping.EventBridgeMapper;
import software.amazon.event.kafkaconnector.util.EventBridgeEventId;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;
import software.amazon.event.kafkaconnector.util.PropertiesUtil;

public class EventBridgeWriter {

  private static final int SDK_TIMEOUT = 5000; // timeout in milliseconds for SDK calls
  private static final Logger log = LoggerFactory.getLogger(EventBridgeWriter.class);

  private final EventBridgeSinkConfig config;
  private final EventBridgeAsyncClient ebClient;
  private final EventBridgeMapper eventBridgeMapper;
  private final EventBridgeBatchingStrategy batching;

  /**
   * @param config Configuration of Sink Client (AWS Region, Eventbus ARN etc.)
   */
  public EventBridgeWriter(EventBridgeSinkConfig config) {
    this.config = config;

    var endpointUri =
        StringUtils.trim(this.config.endpointURI).isBlank()
            ? null
            : URI.create(this.config.endpointURI);

    var retryPolicy =
        RetryPolicy.forRetryMode(RetryMode.STANDARD).toBuilder()
            .numRetries(this.config.maxRetries)
            .build();

    var name = PropertiesUtil.getConnectorName();
    var version = PropertiesUtil.getConnectorVersion();
    var userAgentPrefix = String.format("%s/%s", name, version);

    var clientConfig =
        ClientOverrideConfiguration.builder()
            .retryPolicy(retryPolicy)
            .putAdvancedOption(USER_AGENT_PREFIX, userAgentPrefix)
            .build();

    var credentialsProvider = EventBridgeCredentialsProvider.getCredentials(config);

    var client =
        EventBridgeAsyncClient.builder()
            .region(Region.of(this.config.region))
            .endpointOverride(endpointUri)
            .httpClientBuilder(AwsCrtAsyncHttpClient.builder())
            .overrideConfiguration(clientConfig)
            .credentialsProvider(credentialsProvider)
            .build();

    this.ebClient = client;

    this.eventBridgeMapper = new DefaultEventBridgeMapper(config);
    this.batching = new DefaultEventBridgeBatching();

    log.trace(
        "EventBridgeWriter client config: {}",
        ReflectionToStringBuilder.toString(
            client.serviceClientConfiguration(), ToStringStyle.DEFAULT_STYLE, true));

    // fail fast if credentials cannot be resolved
    log.info("Resolving iam credentials");
    try {
      credentialsProvider.resolveCredentials();
    } catch (Exception e) {
      throw new ConnectException(e);
    }
  }

  /**
   * For testing to inject a custom client
   *
   * @param ebClient Amazon EventBridge client to be used
   * @param config Configuration of Sink Client (AWS Region, Eventbus ARN etc.)
   */
  public EventBridgeWriter(EventBridgeAsyncClient ebClient, EventBridgeSinkConfig config) {
    this.config = config;
    this.ebClient = ebClient;
    this.eventBridgeMapper = new DefaultEventBridgeMapper(config);
    this.batching = new DefaultEventBridgeBatching();
  }

  /**
   * This method ingests data into Amazon EventBridge.
   *
   * @param records The list of {@link org.apache.kafka.connect.sink.SinkRecord}s to be sent to
   *     Amazon EventBridge.
   * @return list of all records with additional status information
   */
  public List<EventBridgeResult<EventBridgeEventId>> putItems(List<SinkRecord> records) {
    var mappingResult = eventBridgeMapper.map(records);

    if (mappingResult.success.isEmpty()) {
      log.warn("Not sending events to EventBridge: no valid records");
      return mappingResult.getErrorsAsResult();
    }

    var sendItemResults =
        batching
            .apply(mappingResult.success.stream())
            .flatMap(this::sendToEventBridge)
            .collect(toList());

    return concat(sendItemResults, mappingResult.getErrorsAsResult());
  }

  private Stream<EventBridgeResult<EventBridgeEventId>> sendToEventBridge(
      List<MappedSinkRecord<PutEventsRequestEntry>> items) {
    try {
      var requestBuilder =
          PutEventsRequest.builder()
              .entries(items.stream().map(MappedSinkRecord::getValue).collect(toList()));

      if (config.endpointID != null && !config.endpointID.isEmpty()) {
        requestBuilder.endpointId(config.endpointID);
      }
      var request = requestBuilder.build();

      log.trace("Sending request to EventBridge: {}", request);
      var response = ebClient.putEvents(request).get(SDK_TIMEOUT, MILLISECONDS);
      log.trace("putEvents response: {}", response.entries());

      if (response.failedEntryCount() > 0) {
        log.warn("Received failed EventBridge entries: {}", response.failedEntryCount());
      }

      return mapResponseToResult(items, response);
    } catch (AwsServiceException
        | SdkClientException
        | ExecutionException
        | InterruptedException
        | TimeoutException e) {

      var cause = e.getCause();
      if (cause instanceof EventBridgeException) {
        var code = ((EventBridgeException) cause).statusCode();
        // entries size limit exceeded
        if (code == HttpStatusCode.REQUEST_TOO_LONG) {
          return items.stream()
              .map(
                  it ->
                      failure(
                          it.getSinkRecord(), reportOnly("EventBridge batch size limit exceeded")));
        }
      }

      return items.stream().map(it -> failure(it.getSinkRecord(), retry(e)));
    } catch (Exception e) {
      return items.stream().map(it -> failure(it.getSinkRecord(), panic(e)));
    }
  }

  public void shutDownEventBridgeClient() {
    ebClient.close();
  }

  private Stream<EventBridgeResult<EventBridgeEventId>> mapResponseToResult(
      List<MappedSinkRecord<PutEventsRequestEntry>> request, PutEventsResponse response) {
    return IntStream.range(0, request.size())
        .mapToObj(
            index -> {
              var sinkRecord = request.get(index).getSinkRecord();
              var resultEntry = response.entries().get(index);

              if (resultEntry.eventId() == null) {
                var code = resultEntry.errorCode();
                var message = resultEntry.errorMessage();
                return failure(sinkRecord, reportOnly(message));
              }
              return success(sinkRecord, EventBridgeEventId.of(resultEntry));
            });
  }

  /**
   * Concat two lists where the elements of the first parameter <code>left</code> are the first
   * elements in the returning list followed by the elements of the second parameter <code>right
   * </code>. The order of the elements does not change.
   *
   * @param left list of elements
   * @param right list of elements
   * @param <T> type of elements
   * @return the concatenation of both given lists
   */
  private static <T> List<T> concat(Collection<T> left, Collection<T> right) {
    return Stream.concat(left.stream(), right.stream()).collect(toList());
  }
}
