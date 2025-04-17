package com.example.payment;

import com.example.payment.FraudEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class PaymentFraudDetector {
    private static final Logger logger = LoggerFactory.getLogger(PaymentFraudDetector.class);
    private static final String INPUT_TOPIC  = "payments";
    private static final String OUTPUT_TOPIC = "fraud";
    private static final double FRAUD_THRESHOLD = 1_000.0;

    public static void main(String[] args) {
        // 1) Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payment-fraud-detector");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5_000);

        // 2) JSON serde using Kafka Connect
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(new JsonSerializer(), new JsonDeserializer());

        // 3) Build topology
        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();

        // 3a) Read raw payments as JsonNode
        KStream<String, JsonNode> paymentsJson = builder.stream(
            INPUT_TOPIC,
            Consumed.with(Serdes.String(), jsonSerde)
        );

        // 3b) Aggregate per-user sums in 1-minute tumbling windows
        TimeWindows windows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1));
        KTable<Windowed<String>, Double> totals = paymentsJson
            .groupBy(
                (key, node) -> node.get("userId").asText(),
                Grouped.with(Serdes.String(), jsonSerde)
            )
            .windowedBy(windows)
            .aggregate(
                () -> 0.0,
                (userId, node, sum) -> sum + node.get("amount").asDouble(),
                Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("user-sums")
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Double())
            );

        // 3c) Emit FraudEvent for users exceeding threshold
        totals.toStream()
            .filter((windowedUser, total) -> total > FRAUD_THRESHOLD)
            .map((windowedUser, total) -> {
                FraudEvent fraudEvent = new FraudEvent(
                    "fraud.detected.v0",
                    windowedUser.key(),
                    total,
                    windowedUser.window().start()
                );
                JsonNode eventNode = mapper.valueToTree(fraudEvent);
                logger.info("fraud detected: {}",eventNode.toPrettyString());
                return new org.apache.kafka.streams.KeyValue<>(windowedUser.key(), eventNode);
            })
            .to(
                OUTPUT_TOPIC,
                Produced.with(Serdes.String(), jsonSerde)
            );

        // 4) Start Kafka Streams
        Topology topology = builder.build();
        logger.info("Starting topology:\n{}", topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.setUncaughtExceptionHandler((thread, error) ->
            logger.error("Stream thread {} failed", thread.getName(), error)
        );
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down streams...");
            streams.close(Duration.ofSeconds(10));
        }));
    }
}
