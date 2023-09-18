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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * This class reads records from an input topic (events), filters events which have a total of over 180 and sends those
 * to an output topic (notifications) using Kafka Streams
 */
public class TransactionAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(TransactionAnalyzer.class);
    private static final Properties props = new Properties();
    private static final Schema.Parser parser = new Schema.Parser();
    private static final CountDownLatch latch = new CountDownLatch(1);

    public static void main(String[] args) {

        var sourceTopic = System.getenv().getOrDefault("TOPIC_NAME", "events");
        var notificationsTopic = System.getenv().getOrDefault("NOTIFICATIONS_TOPIC_NAME", "notificationsTopic");
        var replicationFactor = System.getenv().getOrDefault("REPLICATION_FACTOR", "3");
        var partitionCount = System.getenv().getOrDefault("PARTITION_COUNT", "10");
        var notificationSchemaFile = System.getenv().getOrDefault("NOTIFICATION_SCHEMA_FILE", "notification.avsc");

        var notificationSchema = createSchema(notificationSchemaFile);
        var properties = setConfiguration();

        var topicCreator = new TransactionAnalyzerTopicCreator(properties);
        topicCreator.createTopic(notificationsTopic, Integer.valueOf(partitionCount), Short.valueOf(replicationFactor));
        //Try to create source topic as well, in case Producer is not started yet
        topicCreator.createTopic(sourceTopic, Integer.valueOf(partitionCount), Short.valueOf(replicationFactor));
        topicCreator.close();

        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, GenericRecord> events = builder.stream(sourceTopic);

        events
                .filter((key, value) -> Double.parseDouble(value.get("total").toString()) > 195.00)
                .map((userId, value) -> {
                    var notification = createNotificationRecord(userId, value, notificationSchema);
                    //As this is synthetic test data we can log any value as it does not contain PII
                    log.info("Detected suspicious transaction. Sending notification to topic {}. UserId: {} Value: {}", notificationsTopic, userId, value.get("total").toString());
                    return KeyValue.pair(UUID.randomUUID().toString(), notification);
                })
                .to(notificationsTopic);

        var topology = builder.build();
        var streams = new KafkaStreams(topology, props);


        //Register a shutdown hook to close the Kafka Streams application
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            log.info("Starting Transaction Analyzer ...");
            streams.start();
            latch.await();

        } catch (Throwable e) {
            log.error("Transaction Analyzer failed. Shutting down application.");
            System.exit(1);
        }
        log.info("Transaction Analyzer has completed execution. Shutting down application.");
        System.exit(0);

    }

    /**
     * @return Returns {@link Properties} used for Kafka Streams and Kafka Admin clients
     */
    private static Properties setConfiguration() {
        log.info("Setting Kafka Configuration");

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

        log.debug("Used Kafka Configuration: {}", props);

        return props;
    }

    /**
     * @param schemaFile Name of the .avsc file in the resources folder
     * @return {@link Schema} Return the created schema of the file
     */
    private static Schema createSchema(String schemaFile) {
        log.info("Creating schema for file: {}", schemaFile);
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

    /**
     * @param userId The ID of the user in String format
     * @param value  The transaction record in GenericRecord format
     * @param notificationSchema The schema for the notification record
     * @return {@link GenericRecord} A record with the provided schema
     */
    private static GenericRecord createNotificationRecord(String userId, GenericRecord value,  Schema notificationSchema) {

        var notification = new GenericData.Record(notificationSchema);
        notification.put("source", "transactionAnalyzer");
        notification.put("eventType", "suspiciousActivity");

        var eventDetail = new GenericData.Record(notificationSchema.getField("eventDetail").schema());
        eventDetail.put("userId", userId);
        eventDetail.put("total", Double.parseDouble(value.get("total").toString()));
        notification.put("eventDetail", eventDetail);

        log.debug("Creating notfication record for user {}", userId);

        return notification;
    }

}
