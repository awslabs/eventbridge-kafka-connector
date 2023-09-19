/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */


package software.amazon.event.kafkaconnector;

import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * This class allows the producer to report how many records have been sent in total.
 */
public class ProducerMetricReporter extends AbstractScheduledService {
    private static final Logger log = LogManager.getLogger(ProducerMetricReporter.class);
    private final KafkaProducer<String, GenericRecord> kafkaProducer;
    private final long interval;
    private final TimeUnit timeUnit;

    public ProducerMetricReporter(KafkaProducer<String, GenericRecord> kafkaProducer, long interval, TimeUnit timeUnit) {
        this.kafkaProducer = kafkaProducer;
        this.interval = interval;
        this.timeUnit = timeUnit;

    }

    @Override
    protected void runOneIteration() {

        for (Map.Entry<MetricName, ? extends Metric> entry : kafkaProducer.metrics().entrySet()) {
            if (Objects.equals(entry.getKey().description(), "The total number of records sent.")) {
                log.info(entry.getKey().description() + " " + entry.getValue().metricValue().toString());
            }
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(10, interval, timeUnit);
    }

}
