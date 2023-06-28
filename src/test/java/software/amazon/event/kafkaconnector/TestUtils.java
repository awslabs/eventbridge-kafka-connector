/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

public abstract class TestUtils {

  private TestUtils() {}

  public static final Schema testSchema = SchemaBuilder.struct().field("id", STRING_SCHEMA).build();

  public abstract static class OfSinkRecord {

    private OfSinkRecord() {}

    public static List<SinkRecord> withIdsIn(IntStream range) {
      return range
          .mapToObj(
              id ->
                  new SinkRecord(
                      "topic",
                      0,
                      STRING_SCHEMA,
                      "key",
                      testSchema,
                      new Struct(testSchema).put("id", Integer.toString(id)),
                      id))
          .collect(toList());
    }
  }

  public abstract static class OfPutEventsResultEntry {

    private OfPutEventsResultEntry() {}

    public static List<PutEventsResultEntry> withIdsIn(IntStream range) {
      return range
          .mapToObj(
              id -> PutEventsResultEntry.builder().eventId(String.format("eventId:%d", id)).build())
          .collect(toList());
    }
  }
}
