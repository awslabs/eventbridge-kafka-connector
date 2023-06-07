/*
 *
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 *
 */

package software.amazon.event.kafkaconnector;

import com.amazonaws.services.schemaregistry.kafkastreams.AWSKafkaAvroSerDe;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This class reads records from an input topic (events), filters events which have a total of over 180 and sends those
 * to an output topic (notifications) using Kafka Streams
 */
public class TransactionAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(TransactionAnalyzer.class);
    private static final Properties props = new Properties();
    private static final Schema.Parser parser = new Schema.Parser();


    public static void main(String[] args) {

        var sourceTopic = System.getenv().getOrDefault("TOPIC_NAME", "events");
        var notificationsTopic = System.getenv().getOrDefault("NOTIFICATIONS_TOPIC_NAME", "notifications");
        var replicationFactor = System.getenv().getOrDefault("REPLICATION_FACTOR", "3");
        var partitionCount = System.getenv().getOrDefault("PARTITION_COUNT", "10");
        var notificationSchemaFile = System.getenv().getOrDefault("NOTIFICATION_SCHEMA_FILE", "notification.avsc");

        var notificationSchema = createSchema(notificationSchemaFile);
        var properties = setConfiguration();
        var newNotificationsTopic = createTopic(notificationsTopic, Integer.valueOf(partitionCount), Short.valueOf(replicationFactor));
        var newSourceTopic = createTopic(sourceTopic, Integer.valueOf(partitionCount), Short.valueOf(replicationFactor));


        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, GenericRecord> events = builder.stream(sourceTopic);

       events
                .filter((key, value) -> Double.parseDouble(value.get("total").toString()) > 195.00)
                .map((userId, value) -> {
                    log.info("Sending record downstream ...");
                    var notification = new GenericData.Record(notificationSchema);
                    notification.put("source", "transactionAnalyzer");
                    notification.put("eventType", "suspiciousActivity");

                    var eventDetail = new GenericData.Record(notificationSchema.getField("eventDetail").schema());

                    eventDetail.put("userId", userId);
                    eventDetail.put("total", Double.parseDouble(value.get("total").toString()));
                    notification.put("eventDetail", eventDetail);

                    return KeyValue.pair(UUID.randomUUID().toString(), notification);
                })
                .to(notificationsTopic);

        var streams = new KafkaStreams(builder.build(), props);

        streams.setUncaughtExceptionHandler(exception -> {
            log.error("Kafka-Streams uncaught exception occurred. Shutting down application.");
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });
        log.info("Starting Transaction Analyzer ...");
        streams.start();


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    /**
     * @param topicName         The name of the topic to be created
     * @param partitionCount    The number of partitions for the topic
     * @param replicationFactor The replication factor of the topic. If DEV is true this will be 1
     * @return {@link NewTopic} Return the new topic created
     */
    private static NewTopic createTopic(String topicName, Integer partitionCount, Short replicationFactor) {
        if (System.getenv().getOrDefault("DEV", null) != null) {
            replicationFactor = 1;
        }
        var admin = Admin.create(props);
        var newTopic = new NewTopic(topicName, partitionCount, replicationFactor);

        try {
            var result = admin.createTopics(Collections.singleton(newTopic));
            result.all().get(30, TimeUnit.SECONDS);
            log.info("Topic created: " + topicName);
        } catch (ExecutionException e) {
            if (e.getCause() != null && e.getCause() instanceof TopicExistsException) {
                log.info("Topic exists: {}", newTopic);
            } else {
                log.error("Topic create failed with: " + e);
            }
        } catch (InterruptedException | TimeoutException e) {
            log.error("Topic create failed with: {}", e.getMessage());
        }
        admin.close();
        return newTopic;

    }

    private static Properties setConfiguration() {

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-analyzer-kafka-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AWSKafkaAvroSerDe.class.getName());
        props.put(AWSSchemaRegistryConstants.AWS_REGION, System.getenv().getOrDefault("AWS_REGION", "us-east-1"));
        props.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        props.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        props.put(AWSSchemaRegistryConstants.REGISTRY_NAME, System.getenv().getOrDefault("SCHEMA_REGISTRY_NAME", "streaming"));
        props.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG), "104857600");


        if (System.getenv().getOrDefault("DEV", null) == null) {
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "AWS_MSK_IAM");
            props.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            props.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }

        return props;
    }

    /**
     * @param schemaFile Name of the .avsc file in the resources folder
     * @return {@link Schema} Return the created schema of the file
     */
    private static Schema createSchema(String schemaFile) {
        Schema schema = null;
        try {
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream is = classloader.getResourceAsStream(schemaFile);
            schema = parser.parse(is);
        } catch (IOException e) {
            log.error("Error reading schema file: ", e);
        }
        return schema;
    }


}
