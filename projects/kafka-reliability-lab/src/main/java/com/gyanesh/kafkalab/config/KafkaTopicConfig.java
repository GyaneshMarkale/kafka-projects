package com.gyanesh.kafkalab.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    public static final String ORDERS_TOPIC = "orders.events";
    public static final String DLT_TOPIC = "orders.events.DLT";
    public static final int PARTITIONS = 6;

    @Bean
    NewTopic orderEventsTopic() {
        return TopicBuilder.name(ORDERS_TOPIC).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic orderEventsDeadLetterTopic() {
        return TopicBuilder.name(DLT_TOPIC).partitions(PARTITIONS).replicas(1).build();
    }
}
