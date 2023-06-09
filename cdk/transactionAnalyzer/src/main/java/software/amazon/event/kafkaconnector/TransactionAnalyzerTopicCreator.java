/*
 *
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 *
 */

package software.amazon.event.kafkaconnector;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TransactionAnalyzerTopicCreator {
    private final Logger log = LoggerFactory.getLogger(TransactionAnalyzerTopicCreator.class);

    private final Properties properties;

    public TransactionAnalyzerTopicCreator(Properties properties) {
        this.properties = properties;
    }

    /**
     * @param topicName         The name of the topic to be created
     * @param partitionCount    The number of partitions for the topic
     * @param replicationFactor The replication factor of the topic. If DEV is true this will be 1
     */
    public void createTopic(String topicName, Integer partitionCount, Short replicationFactor) {
        log.info("Creating topic: {}", topicName);
        if (System.getenv().getOrDefault("DEV", null) != null) {
            replicationFactor = 1;
        }
        var admin = Admin.create(properties);
        var newTopic = new NewTopic(topicName, partitionCount, replicationFactor);

        try {
            var result = admin.createTopics(Collections.singleton(newTopic));
            result.all().get(30, TimeUnit.SECONDS);
            log.info("Topic created: " + topicName);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            if (e.getCause() != null && e.getCause() instanceof TopicExistsException) {
                log.info("Topic exists: {}", newTopic);
            } else {
                log.error("Topic create failed with: " + e);
            }
        }
        admin.close();
    }

}
