package com.example.payment;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.payment.Payment;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Produces Payment records to the 'payments' topic using Kafka Connect's JSON serializer.
 */
public class PaymentProducer implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(PaymentProducer.class);
    private static final String TOPIC = "payments";
    private final KafkaProducer<String, JsonNode> producer;
    private final ObjectMapper mapper = new ObjectMapper();

    public PaymentProducer() {
        Properties props = new Properties();

        // Load defaults from producer.properties on the classpath
        try (InputStream in = getClass().getClassLoader()
                .getResourceAsStream("producer.properties")) {

            if (in == null) {
                throw new IllegalStateException("producer.properties not found on classpath");
            }
            props.load(in);

        } catch (IOException e) {
            throw new UncheckedIOException("Failed to load producer.properties", e);
        }

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // Instantiate Kafka producer
        this.producer = new KafkaProducer<>(props);
        logger.info("Initialized KafkaProducer for topic '{}' with Connect JSON serializer", TOPIC);
    }

    /**
     * Convert Payment to JsonNode and send.
     */
    public void send(Payment payment) {
        JsonNode node = mapper.valueToTree(payment);
        ProducerRecord<String, JsonNode> record = new ProducerRecord<>(TOPIC, payment.paymentId(), node);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to send payment {}: {}", payment.paymentId(), exception.getMessage(), exception);
            } else {
                logger.info("Sent payment {} to partition {} at offset {}",
                        payment.paymentId(), metadata.partition(), metadata.offset());
            }
        });
    }

    @Override
    public void close() {
        logger.info("Flushing and closing KafkaProducer");
        producer.flush();
        producer.close();
    }

    public static void main(String[] args) {
        Random random = new Random();
        String[] users = IntStream.range(1, 11)
                .mapToObj(i -> "user-" + i)
                .toArray(String[]::new);
        String[] merchants = IntStream.range(1, 6)
                .mapToObj(i -> "merchant-" + i)
                .toArray(String[]::new);

        try (PaymentProducer app = new PaymentProducer()) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down payment producer...");
                app.close();
            }));

            logger.info("Starting payment producer. Press CTRL+C to stop.");

            while (true) {
                Payment p = new Payment(
                        UUID.randomUUID().toString(),
                        users[random.nextInt(users.length)],
                        System.currentTimeMillis(),
                        Math.round(random.nextDouble() * 1000 * 100.0) / 100.0,
                        "EUR",
                        merchants[random.nextInt(merchants.length)]
                );

                app.send(p);

                logger.info("Generated payment {} for user {} to merchant {}",
                        p.paymentId(), p.userId(), p.merchant());

                try {
                    Thread.sleep(random.nextInt(1900) + 100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Payment producer interrupted, exiting loop");
                    break;
                }
            }
        } catch (Exception e) {
            logger.error("Unexpected error in payment producer", e);
        }
    }
}
