/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.event.kafkaconnector.exceptions.EventBridgeWriterException.ExceptionType.NON_RETRYABLE;
import static software.amazon.event.kafkaconnector.exceptions.EventBridgeWriterException.ExceptionType.RETRYABLE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.json.JsonConverter;
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
import software.amazon.event.kafkaconnector.exceptions.EventBridgeWriterException;
import software.amazon.event.kafkaconnector.util.PropertiesUtil;

public class EventBridgeWriter {

  private static final int SDK_TIMEOUT = 5000; // timeout in milliseconds for SDK calls
  private static final String sourcePrefix = "kafka-connect.";
  private final EventBridgeSinkConfig config;
  private final EventBridgeAsyncClient ebClient;
  private final JsonConverter jsonConverter = new JsonConverter();
  private final Logger log = LoggerFactory.getLogger(EventBridgeWriter.class);
  private final ObjectMapper objectMapper = new ObjectMapper();

  /**
   * @param config Configuration of Sink Client (AWS Region, Eventbus ARN etc.)
   */
  public EventBridgeWriter(EventBridgeSinkConfig config) {

    this.config = config;
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);

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

    var client =
        EventBridgeAsyncClient.builder()
            .region(Region.of(this.config.region))
            .endpointOverride(endpointUri)
            .httpClientBuilder(AwsCrtAsyncHttpClient.builder())
            .overrideConfiguration(clientConfig)
            .credentialsProvider(EventBridgeCredentialsProvider.getCredentials(config))
            .build();

    this.ebClient = client;

    log.trace(
        "EventBridgeWriter client config: {}",
        ReflectionToStringBuilder.toString(
            client.serviceClientConfiguration(), ToStringStyle.DEFAULT_STYLE, true));
  }

  /**
   * For testing to inject a custom client
   *
   * @param ebClient Amazon EventBridge client to be used
   * @param config
   */
  public EventBridgeWriter(EventBridgeAsyncClient ebClient, EventBridgeSinkConfig config) {
    this.config = config;
    this.ebClient = ebClient;
    jsonConverter.configure(Collections.singletonMap("schemas.enable", "false"), false);
  }

  /**
   * This method ingests data into Amazon EventBridge.
   *
   * @param records
   * @return list of all records with additional status information
   */
  public List<EventBridgeWriterRecord> putItems(List<EventBridgeWriterRecord> records) {
    try {
      var putEventsEntries =
          records.stream()
              .map(this::createPutEventsEntry)
              .flatMap(Optional::stream)
              .collect(Collectors.toList());

      if (putEventsEntries.size() == 0) {
        log.warn(
            "Not sending events to eventbridge: no valid entries for putevents call for eventbridge "
                + "writer records");
        return records;
      }

      var request = PutEventsRequest.builder().entries(putEventsEntries).build();
      log.trace("EventBridgeWriter sending request to eventbridge: {}", request);
      var response = ebClient.putEvents(request).get(SDK_TIMEOUT, MILLISECONDS);
      log.trace("EventBridgeWriter putEvents response: {}", response.entries());

      // Check for errors and return index of failed items
      if (response.failedEntryCount() > 0) {
        log.warn("Received failed eventbridge entries: {}", response.failedEntryCount());
        markFailedRecordsFromResponse(records, response);
      }
      return records;
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
          log.warn("Caught eventbridge exception: batch size limit exceeded", cause);
          markAllRecordsFailed(records, "413", "EventBridge batch size limit exceeded");
          return records;
        }
      }

      // all other cases assume transient EventBridge service or network error
      throw new EventBridgeWriterException(e, RETRYABLE);
    } catch (Exception e) {
      throw new EventBridgeWriterException(e, NON_RETRYABLE);
    }
  }

  public void shutDownEventBridgeClient() {
    ebClient.close();
  }

  /**
   * A helper method to build the record which is sent to Amazon EventBridge
   *
   * @param record Record to be sent to EventBridge
   * @return the PutEventsRequestEntry ready to send
   */
  private Optional<PutEventsRequestEntry> createPutEventsEntry(EventBridgeWriterRecord record) {
    try {
      return Optional.of(
          PutEventsRequestEntry.builder()
              .eventBusName(config.eventBusArn)
              .source(sourcePrefix + config.connectorId)
              .detailType(config.getDetailType(record.getTopic()))
              .resources(config.resources)
              .detail(createJsonPayload(record.getSinkRecord()))
              .build());
    } catch (DataException e) {
      record.setError("422", "Cannot convert kafka record to eventbridge event: " + e.getMessage());
      return Optional.empty();
    }
  }

  /**
   * This method sets the status (error) information on each record that has been sent to Amazon
   * EventBridge. It sets the properties errorCode and errorMessage on the {@link
   * EventBridgeWriterRecord}. To correlate the initial records with the responses the index is
   * used.
   *
   * @param records List of {@link EventBridgeWriterRecord} that were sent (or failed)
   * @param putEventsResponse Response from {@link
   *     software.amazon.awssdk.services.eventbridge.EventBridgeClient#putEvents(PutEventsRequest)}
   */
  private void markFailedRecordsFromResponse(
      List<EventBridgeWriterRecord> records, PutEventsResponse putEventsResponse) {
    for (int i = 0; i < putEventsResponse.entries().size(); i++) {
      var currentEntry = putEventsResponse.entries().get(i);
      if (currentEntry.eventId() == null) {
        var code = currentEntry.errorCode();
        var message = currentEntry.errorMessage();
        var record = records.get(i);
        log.warn(
            "Marking record as failed: code={} message={} topic={} partition={} offset={}",
            code,
            message,
            record.getTopic(),
            record.getPartition(),
            record.getOffset());
        record.setError(currentEntry.errorCode(), currentEntry.errorMessage());
      }
    }
  }

  /**
   * This method sets the status (error) information on all records that have been sent to Amazon
   * EventBridge. It sets the properties errorCode and errorMessage on the {@link
   * EventBridgeWriterRecord}.
   *
   * @param records List of {@link EventBridgeWriterRecord} that were sent (or failed)
   * @param errorCode
   * @param errorMessage
   */
  private void markAllRecordsFailed(
      List<EventBridgeWriterRecord> records, String errorCode, String errorMessage) {

    records.forEach(
        record -> {
          log.warn(
              "Marking record as failed: code={} message={} topic={} partition={} offset={}",
              errorCode,
              errorMessage,
              record.getTopic(),
              record.getPartition(),
              record.getOffset());
          record.setError(errorCode, errorMessage);
        });
  }

  /**
   * This method serializes the Kafka record to JSON format, which is the required format for
   * EventBridge.
   *
   * @param record Kafka record to be sent to EventBridge
   * @return string representation of the event sent to EventBridge
   */
  private String createJsonPayload(SinkRecord record) {
    try {
      var root = objectMapper.createObjectNode();
      root.put("topic", record.topic());
      root.put("partition", record.kafkaPartition());
      root.put("offset", record.kafkaOffset());
      root.put("timestamp", record.timestamp());
      root.put("timestampType", record.timestampType().toString());
      root.putIfAbsent("headers", createHeaderArray(record));

      if (record.key() == null) {
        root.putIfAbsent("key", null);
      } else {
        root.putIfAbsent(
            "key",
            createJSONFromByteArray(
                jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key())));
      }

      // tombstone handling
      if (record.value() == null) {
        root.putIfAbsent("value", null);
      } else {
        root.putIfAbsent(
            "value",
            createJSONFromByteArray(
                jsonConverter.fromConnectData(
                    record.topic(), record.valueSchema(), record.value())));
      }

      return root.toString();
    } catch (Exception e) {
      throw new DataException("Record can not be converted to json", e);
    }
  }

  /**
   * This method serializes Kafka message headers to JSON.
   *
   * @param record Kafka record to be sent to EventBridge
   * @return headers to be added to EventBridge message
   * @throws JsonProcessingException
   */
  private ArrayNode createHeaderArray(SinkRecord record) throws JsonProcessingException {
    var headersArray = objectMapper.createArrayNode();

    for (Header header : record.headers()) {
      var headerItem = objectMapper.createObjectNode();
      headerItem.putIfAbsent(
          header.key(),
          createJSONFromByteArray(
              jsonConverter.fromConnectHeader(
                  record.topic(), header.key(), header.schema(), header.value())));
      headersArray.add(headerItem);
    }
    return headersArray;
  }

  /**
   * This method converts the byteArray which is returned by the {@link JsonConverter} to JSON.
   *
   * @param jsonBytes - byteArray to convert to JSON
   * @return the JSON representation of jsonBytes
   * @throws JsonProcessingException
   */
  private JsonNode createJSONFromByteArray(byte[] jsonBytes) throws JsonProcessingException {
    return objectMapper.readTree(new String(jsonBytes, StandardCharsets.UTF_8));
  }
}
