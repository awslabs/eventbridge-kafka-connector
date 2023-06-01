/*
 *
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 *
 */

package software.amazon.event.kafkaconnector;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.google.common.util.concurrent.ServiceManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TransactionAnalyzer {
    private static final Logger log = LoggerFactory.getLogger(TransactionAnalyzer.class);
    private static final Properties properties = new Properties();
    private static final Schema.Parser parser = new Schema.Parser();

    public static void main(String[] args) {
        var topic = System.getenv().getOrDefault("TOPIC_NAME", "events");
        var kafkaConsumer = createKafkaConsumer();

        var transactionProcessor = new TransactionProcessor(kafkaConsumer, topic);
        transactionProcessor.startAsync();

        transactionProcessor.awaitTerminated();

        kafkaConsumer.close();

    }

    private static KafkaConsumer<String, GenericRecord> createKafkaConsumer() {

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, System.getenv().getOrDefault("CONSUMER_GROUP_ID","transactionAnalyzer"));
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

        return new KafkaConsumer<>(properties);
    }
}
