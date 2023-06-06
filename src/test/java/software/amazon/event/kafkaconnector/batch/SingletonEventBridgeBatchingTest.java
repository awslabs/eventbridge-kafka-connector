/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.function.Function;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

public class SingletonEventBridgeBatchingTest {

  private static final EventBridgeBatchingStrategy strategy = new SingletonEventBridgeBatching();

  private static final Function<List<MappedSinkRecord<PutEventsRequestEntry>>, List<String>>
      sinkRecordId =
          xs ->
              xs.stream()
                  .map(it -> ((Struct) it.getSinkRecord().value()).get("id").toString())
                  .collect(toList());

  private static final String[] Ids = {
    "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"
  };

  @ParameterizedTest(
      name =
          "{index} should batch provided list of {0} element(s) into list with {0} singleton lists in same order")
  @ValueSource(ints = {0, 1, 2, 5, 10})
  public void shouldBatchListOfSize(int size) {
    var input = range(0, size).mapToObj(index -> createMappedSinkRecord(Ids[index]));
    var expected = range(0, size).mapToObj(index -> singletonList(Ids[index])).collect(toList());

    assertThat(strategy.apply(input)).extracting(sinkRecordId).hasSameElementsAs(expected);
  }

  private MappedSinkRecord<PutEventsRequestEntry> createMappedSinkRecord(String id) {
    return new MappedSinkRecord<>(
        createTestRecordWithId(id), PutEventsRequestEntry.builder().build());
  }

  private SinkRecord createTestRecordWithId(String id) {
    var valueSchema = SchemaBuilder.struct().field("id", Schema.STRING_SCHEMA).build();
    var value = new Struct(valueSchema).put("id", id);

    return new SinkRecord("topic", 0, Schema.STRING_SCHEMA, "key", valueSchema, value, 0);
  }
}
