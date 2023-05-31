/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package software.amazon.event.kafkaconnector;

import com.github.javafaker.Faker;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class ProducerEventSender extends AbstractExecutionThreadService {

    private static final Logger log = LogManager.getLogger(ProducerEventSender.class);
    private final KafkaProducer<String, GenericRecord> producer;
    private final Schema schema;
    private final String topic;
    private final Faker faker = new Faker();


    public ProducerEventSender(KafkaProducer<String, GenericRecord> producer, Schema schema, String topic) {
        this.producer = producer;
        this.schema = schema;
        this.topic = topic;
    }

    private ProducerRecord<String, GenericRecord> createEvent() {
        var event = new GenericData.Record(schema);
        event.put("id", faker.app().version());
        event.put("creditCard", faker.finance().creditCard());
        event.put("firstName", faker.name().firstName());
        event.put("lastName", faker.name().lastName());
        event.put("streetAddress", faker.address().lastName());

        return new ProducerRecord<>(topic, UUID.randomUUID().toString(), event);
    }

    @Override
    protected void run() throws ExecutionException, InterruptedException {
        log.info("Running producer thread ...");
        while (true) {
            var producerRecord = createEvent();
            producer.send(producerRecord).get();
            Thread.sleep(500);
        }

    }

}
