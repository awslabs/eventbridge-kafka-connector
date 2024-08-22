/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static software.amazon.event.kafkaconnector.EventBridgeResult.Error.reportOnly;
import static software.amazon.event.kafkaconnector.EventBridgeResult.failure;
import static software.amazon.event.kafkaconnector.EventBridgeResult.success;

import java.util.List;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.event.kafkaconnector.EventBridgeResult;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;

public class DefaultEventBridgeMapper implements EventBridgeMapper {

  private static final String sourcePrefix = "kafka-connect.";

  private final EventBridgeSinkConfig config;

  private final SinkRecordJsonMapper jsonMapper = new SinkRecordJsonMapper();

  private final DetailTypeMapper detailTypeMapper;
  private final TimeMapper timeMapper;

  public DefaultEventBridgeMapper(EventBridgeSinkConfig config) {

    this.config = config;
    this.detailTypeMapper = getDetailTypeMapper(config);
    this.timeMapper = getTimeMapper(config);
  }

  public EventBridgeMappingResult map(List<SinkRecord> records) {
    var partition =
        records.stream()
            .map(this::createPutEventsEntry)
            .collect(partitioningBy(EventBridgeResult::isSuccess));

    var successfulMappedRecords =
        partition.get(true).stream().map(EventBridgeResult::success).collect(toList());
    var failedMappedRecords =
        partition.get(false).stream().map(EventBridgeResult::failure).collect(toList());

    return new EventBridgeMappingResult(successfulMappedRecords, failedMappedRecords);
  }

  private EventBridgeResult<PutEventsRequestEntry> createPutEventsEntry(SinkRecord record) {
    try {
      return success(
          record,
          PutEventsRequestEntry.builder()
              .eventBusName(config.eventBusArn)
              .source(sourcePrefix + config.connectorId)
              .detailType(detailTypeMapper.getDetailType(record))
              .resources(config.resources)
              .detail(jsonMapper.createJsonPayload(record))
              .time(timeMapper.getTime(record))
              .build());
    } catch (Exception e) {
      return failure(record, reportOnly("Cannot convert Kafka record to EventBridge.", e));
    }
  }

  private DetailTypeMapper getDetailTypeMapper(EventBridgeSinkConfig config) {
    try {
      var myClass = Class.forName(config.detailTypeMapperClass);
      var constructor = myClass.getDeclaredConstructor();
      var detailTypeMapper = (DetailTypeMapper) constructor.newInstance();
      detailTypeMapper.configure(config);
      return detailTypeMapper;
    } catch (Exception e) {
      // This will already be verified in the Config Validator
      throw new RuntimeException("Topic to Detail-Type Mapper Class can't be loaded.");
    }
  }

  private TimeMapper getTimeMapper(EventBridgeSinkConfig config) {
    try {
      var myClass = Class.forName(config.timeMapperClass);
      var constructor = myClass.getDeclaredConstructor();
      return (TimeMapper) constructor.newInstance();
    } catch (Exception e) {
      // This will already be verified in the Config Validator
      throw new RuntimeException("Time Mapper Class can't be loaded.");
    }
  }
}
