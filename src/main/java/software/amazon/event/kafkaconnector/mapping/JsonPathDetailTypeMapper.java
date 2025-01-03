/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import static com.jayway.jsonpath.Configuration.defaultConfiguration;
import static com.jayway.jsonpath.Option.SUPPRESS_EXCEPTIONS;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static software.amazon.event.kafkaconnector.EventBridgeSinkConfig.AWS_DETAIL_TYPES_MAPPER_JSON_PATH_MAPPER_FIELDREF;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import software.amazon.event.kafkaconnector.EventBridgeSinkConfig;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;

public class JsonPathDetailTypeMapper implements DetailTypeMapper {
  private static final Logger log =
      ContextAwareLoggerFactory.getLogger(JsonPathDetailTypeMapper.class);

  private static final Configuration jsonPathConfiguration =
      defaultConfiguration()
          .addOptions(
              // suppress exception otherwise com.jayway.jsonpath.ReadContext#read throws an
              // exception if JSON path could not be found
              SUPPRESS_EXCEPTIONS);

  private String jsonPath;
  private final JsonConverter jsonConverter = new JsonConverter();

  public JsonPathDetailTypeMapper() {
    this.jsonConverter.configure(singletonMap(SCHEMAS_ENABLE_CONFIG, "false"), false);
  }

  @Override
  public String getDetailType(SinkRecord record) {
    if (record == null) {
      throw new IllegalArgumentException("SinkRecord is null. Unable to extract detail type.");
    }

    if (record.topic() == null || record.topic().trim().isEmpty()) {
      throw new IllegalArgumentException(
          "SinkRecord topic is null or empty but is required for fallback logic.");
    }

    var topic = record.topic();
    try {
      var jsonBytes = jsonConverter.fromConnectData(topic, record.valueSchema(), record.value());
      // super defensive, because null here should never be the case
      var jsonString = new String(jsonBytes).trim();
      if (jsonBytes == null || jsonString.isEmpty()) {
        log.error(
            "Record value conversion to JSON bytes returned null or empty string for record '{}', using topic '{}' as"
                + " fallback",
            record,
            topic);
        return topic;
      }

      var extractedValue = JsonPath.using(jsonPathConfiguration).parse(jsonString).read(jsonPath);

      if (extractedValue == null) {
        log.warn(
            "Parsed JSON value is null for JSON path '{}' and record '{}', using topic '{}' as fallback",
            jsonPath,
            record,
            topic);
        return topic;
      }

      if (!(extractedValue instanceof String)) {
        log.warn(
            "Parsed JSON value is not of type String for for JSON path '{}' and record '{}', using topic '{}' as fallback",
            jsonPath,
            record,
            topic);
        return topic;
      }

      if (((String) extractedValue).trim().isEmpty()) {
        log.warn(
            "Parsed JSON value is empty String for JSON path '{}' and record '{}', using topic '{}' as fallback",
            jsonPath,
            record,
            topic);
        return topic;
      }

      log.trace(
          "Successfully extracted detail type '{}' for JSON path '{}' and record '{}'",
          extractedValue,
          jsonPath,
          record);
      return (String) extractedValue;
    } catch (Exception e) {
      log.error(
          "Could not extract JSON value for JSON path '{}' and record '{}', using topic '{}' as fallback",
          jsonPath,
          record,
          topic);
      return topic;
    }
  }

  @Override
  public void configure(EventBridgeSinkConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("EventBridgeSinkConfig cannot be null.");
    }

    var jsonPath = config.getString(AWS_DETAIL_TYPES_MAPPER_JSON_PATH_MAPPER_FIELDREF);

    if (jsonPath == null || jsonPath.trim().isEmpty()) {
      throw new IllegalArgumentException(
          "JSON path configuration must be provided and cannot be empty.");
    }

    var path = JsonPath.compile(jsonPath);
    if (!path.isDefinite()) {
      throw new IllegalArgumentException(
          format("JSON path must be definite but '%s' is not", jsonPath));
    }

    this.jsonPath = path.getPath();
    log.info("JsonPathDetailTypeMapper configured successfully with JSON path '{}'", this.jsonPath);
  }
}
