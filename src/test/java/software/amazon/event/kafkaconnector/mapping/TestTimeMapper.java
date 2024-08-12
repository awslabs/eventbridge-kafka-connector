/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.mapping;

import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.time.Instant;
import org.apache.kafka.connect.sink.SinkRecord;

public class TestTimeMapper implements TimeMapper {

  @Override
  public Instant getTime(SinkRecord record) {
    try {
      String jsonPayload = new SinkRecordJsonMapper().createJsonPayload(record);
      String fieldValue = JsonPath.read(jsonPayload, "$.value.timeField").toString();
      return Instant.parse(fieldValue);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
