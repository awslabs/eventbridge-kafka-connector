/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.event.kafkaconnector;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Producer {

    private static final Logger log = LogManager.getLogger(Producer.class);

    private static final Properties properties = new Properties();
    private static final Schema.Parser parser = new Schema.Parser();


    public static void main(String[] args) {

        var numberOfProducers = Integer.parseInt(System.getenv().getOrDefault("NUMBER_OF_PRODUCERS", "1"));
        var topic = System.getenv().getOrDefault("TOPIC_NAME", "events");
        var schemaFile = System.getenv().getOrDefault("SCHEMA_FILE","payload.avsc");
        var replicationFactor = System.getenv().getOrDefault("REPLICATION_FACTOR", "3");
        var partitionCount = System.getenv().getOrDefault("PARTITION_COUNT", "10");


        var kafkaProducer = createKafkaProducer();
        var schema = createSchema(schemaFile);
        var newTopic = createTopic(topic, Integer.valueOf(partitionCount), Short.valueOf(replicationFactor));

        var producerMetricReporter = new ProducerMetricReporter(kafkaProducer, 10, TimeUnit.SECONDS);
        producerMetricReporter.startAsync();

        var services = Collections.nCopies(numberOfProducers, new ProducerEventSender(kafkaProducer, schema, topic));

        var serviceManager = new ServiceManager(services);

        serviceManager.startAsync();

        serviceManager.awaitStopped();
        producerMetricReporter.stopAsync();
        kafkaProducer.flush();
        kafkaProducer.close();

    }

    private static KafkaProducer<String, GenericRecord> createKafkaProducer() {

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        properties.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        properties.put(AWSSchemaRegistryConstants.AWS_REGION, System.getenv().getOrDefault("AWS_REGION", "us-east-1"));
        properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, System.getenv().getOrDefault("SCHEMA_REGISTRY_NAME", "default-registry"));
        properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, System.getenv().getOrDefault("TOPIC_NAME", "events"));
        properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);

        if (System.getenv().getOrDefault("DEV", null) == null) {
            properties.put("security.protocol", "SASL_SSL");
            properties.put("sasl.mechanism", "AWS_MSK_IAM");
            properties.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            properties.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }

        return new KafkaProducer<>(properties);
    }

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

    private static NewTopic createTopic(String topicName, Integer partitionCount, Short replicationFactor) {
        if (System.getenv().getOrDefault("DEV", null) != null) {
            replicationFactor = 1;
        }
        var admin = Admin.create(properties);
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
            log.error(e);
            log.error("Topic create failed with: {}", e.getMessage());
        }
        admin.close();
        return  newTopic;

    }


}
