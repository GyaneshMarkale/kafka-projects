package com.gyanesh.kafkalab.producer;

import com.gyanesh.kafkalab.config.KafkaTopicConfig;
import com.gyanesh.kafkalab.events.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventProducer {
    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public OrderEventProducer(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<Object, Object>> publish(String key, OrderEvent event) {
        return kafkaTemplate.send(KafkaTopicConfig.ORDERS_TOPIC, key, event)
                .whenComplete((result, failure) -> {
                    if (failure != null) {
                        log.error("Kafka publish failed eventId={} key={}", event.eventId(), key, failure);
                    } else {
                        log.debug("Kafka publish ack eventId={} partition={} offset={}",
                                event.eventId(), result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
