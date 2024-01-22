/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector.batch;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;
import org.slf4j.Logger;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.event.kafkaconnector.logging.ContextAwareLoggerFactory;
import software.amazon.event.kafkaconnector.util.MappedSinkRecord;

/**
 * Default batching strategy for a stream of {@link MappedSinkRecord}&lt;{@link
 * PutEventsRequestEntry}&gt; (equipped with the associated <code>SinkRecord</code>) to send to
 * EventBridge. The strategy generates a stream of lists of <code>PutEventsRequestEntry</code>}
 * (batch) in which every batch:
 *
 * <ul>
 *   <li>contains at most 10 items with overall size &le; 256Kb according to this <a
 *       href="https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevent-size.html">calculation</a>,
 *       or
 *   <li>one item which size is &ge; 256Kb to isolate events exceeding the EventBridge event size
 *       limit which are later handled by PutEvents calls e.g., dropping or sending to a dead-letter
 *       topic if configured.
 * </ul>
 *
 * If a batch contains an <code>PutEventsRequestEntry</code> &gt; 256Kb then a log message is
 * emitted with level <code>WARN</code> on logger <code>
 * software.amazon.event.kafkaconnector.batch.DefaultEventBridgeBatching</code>.
 *
 * @author Andreas Gebhardt
 * @since 1.1.0
 */
public class DefaultEventBridgeBatching implements EventBridgeBatchingStrategy {

  private static final Logger logger =
      ContextAwareLoggerFactory.getLogger(DefaultEventBridgeBatching.class);

  private final Collector<
          MappedSinkRecord<PutEventsRequestEntry>,
          Accumulator,
          Stream<List<MappedSinkRecord<PutEventsRequestEntry>>>>
      collector = new PutEventsRequestEntrySizeCollector();

  /**
   * Generates a stream of batches from the provided input by the implemented strategy.
   *
   * @param records stream of {@link MappedSinkRecord}&lt;{@link PutEventsRequestEntry}&gt; where
   *     batching will be applied
   * @return stream of batched {@link MappedSinkRecord}&lt;{@link PutEventsRequestEntry}&gt; where
   *     each batch is either limited by its accumulated byte size or by the maximum number of items
   * @throws IllegalArgumentException if input stream is parallel
   */
  @Override
  public Stream<List<MappedSinkRecord<PutEventsRequestEntry>>> apply(
      Stream<MappedSinkRecord<PutEventsRequestEntry>> records) {
    if (records.isParallel()) {
      throw new IllegalArgumentException("Stream must not be parallel.");
    }
    return records.collect(collector);
  }

  static class PutEventsRequestEntrySizeCollector
      implements Collector<
          MappedSinkRecord<PutEventsRequestEntry>,
          Accumulator,
          Stream<List<MappedSinkRecord<PutEventsRequestEntry>>>> {

    @Override
    public Supplier<Accumulator> supplier() {
      return Accumulator::new;
    }

    @Override
    public BiConsumer<Accumulator, MappedSinkRecord<PutEventsRequestEntry>> accumulator() {
      return Accumulator::accumulate;
    }

    @Override
    public BinaryOperator<Accumulator> combiner() {
      return (left, right) -> {
        throw new IllegalStateException();
      };
    }

    @Override
    public Function<Accumulator, Stream<List<MappedSinkRecord<PutEventsRequestEntry>>>> finisher() {
      return Accumulator::finish;
    }

    @Override
    public Set<Characteristics> characteristics() {
      return emptySet();
    }
  }

  static class Accumulator {

    private static final int MAX_BATCH_SIZE_BYTES = 256 * 1024;
    private static final int MAX_BATCH_ITEMS = 10;

    private final List<List<MappedSinkRecord<PutEventsRequestEntry>>> batches = new ArrayList<>();
    private int actualBatchSize = 0;
    private int index = 0;

    Accumulator() {
      batches.add(new ArrayList<>());
    }

    void accumulate(final MappedSinkRecord<PutEventsRequestEntry> item) {

      var itemSize = getSize(item.getValue());
      if (itemSize > MAX_BATCH_SIZE_BYTES) {
        var sinkRecord = item.getSinkRecord();
        logger.warn(
            "Item for SinkRecord with topic='{}', partition={} and offset={} exceeds EventBridge size limit. Size is {} bytes.",
            sinkRecord.topic(),
            sinkRecord.kafkaPartition(),
            sinkRecord.kafkaOffset(),
            itemSize);
      }
      var actualBatchItems = batches.get(index).size();
      if ((actualBatchSize + itemSize > MAX_BATCH_SIZE_BYTES) && (actualBatchItems > 0)
          || (actualBatchItems >= MAX_BATCH_ITEMS)) {
        batches.add(new ArrayList<>());
        index++;
        actualBatchSize = 0;
      }

      actualBatchSize += itemSize;
      batches.get(index).add(item);
    }

    Stream<List<MappedSinkRecord<PutEventsRequestEntry>>> finish() {
      return ((batches.size() == 1) && (actualBatchSize == 0)) ? Stream.empty() : batches.stream();
    }
  }

  // https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevent-size.html
  static int getSize(PutEventsRequestEntry entry) {
    var size = 0;
    if (entry.time() != null) {
      size += 14;
    }
    size += entry.source().getBytes(UTF_8).length;
    size += entry.detailType().getBytes(UTF_8).length;
    if (entry.detail() != null) {
      size += entry.detail().getBytes(UTF_8).length;
    }
    if (entry.resources() != null) {
      for (String resource : entry.resources()) {
        if (resource != null) {
          size += resource.getBytes(UTF_8).length;
        }
      }
    }
    if (entry.traceHeader() != null) {
      size += entry.traceHeader().getBytes(UTF_8).length;
    }
    return size;
  }
}
