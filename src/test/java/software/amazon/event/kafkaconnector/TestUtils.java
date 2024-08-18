/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.event.kafkaconnector;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.services.eventbridge.model.PutEventsResultEntry;

public abstract class TestUtils {

  private TestUtils() {}

  public static final Schema testSchema = SchemaBuilder.struct().field("id", STRING_SCHEMA).build();

  public static final Function<String, String> toFixedGitCommitId =
      it -> it.replaceAll("\\[@[0-9a-f]+(-dirty)?]", "[GitCommitId]");

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

  public static class ListAppender extends AppenderBase<ILoggingEvent> {

    private final Logger logger;

    public ListAppender(final Logger logger) {
      this.logger = logger;
    }

    private final List<ILoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(ILoggingEvent event) {
      synchronized (this) {
        events.add(event);
      }
    }

    public List<ILoggingEvent> getLoggingEvents() {
      synchronized (this) {
        return new ArrayList<>(events);
      }
    }

    public void clear() {
      synchronized (this) {
        events.clear();
      }
    }

    public void detach() {
      synchronized (this) {
        logger.detachAppender(this);
      }
    }

    public static ListAppender of(Class<?> clazz, Level level) {
      var logger = (Logger) LoggerFactory.getLogger(clazz);
      logger.setLevel(level);

      final var appender = new ListAppender(logger);
      appender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());

      logger.addAppender(appender);
      return appender;
    }
  }

  public static class NonAwsCredentialProvider {}

  public static class NoNoArgAwsCredentialProvider implements AwsCredentialsProvider {

    private final String sentinel;

    public NoNoArgAwsCredentialProvider(String sentinel) {
      this.sentinel = sentinel;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return null;
    }

    @Override
    public String toString() {
      return "NoNoArgAwsCredentialProvider{" + "sentinel='" + sentinel + '\'' + '}';
    }
  }
}
