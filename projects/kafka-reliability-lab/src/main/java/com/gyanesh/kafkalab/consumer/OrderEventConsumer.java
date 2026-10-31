package com.gyanesh.kafkalab.consumer;

import com.gyanesh.kafkalab.config.KafkaTopicConfig;
import com.gyanesh.kafkalab.core.DeliveryFailurePolicy;
import com.gyanesh.kafkalab.events.OrderEvent;
import com.gyanesh.kafkalab.service.EventProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class OrderEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(OrderEventConsumer.class);
    private final EventProcessingService processingService;

    public OrderEventConsumer(EventProcessingService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(topics = KafkaTopicConfig.ORDERS_TOPIC, groupId = "order-processing-v1")
    public void consume(
            OrderEvent event,
            @Header(name = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer attemptHeader) {
        int attempt = attemptHeader == null ? 1 : attemptHeader;
        if (DeliveryFailurePolicy.shouldFail(attempt, event.failuresBeforeSuccess(), event.poisonPill())) {
            log.warn("SIMULATED_FAILURE eventId={} deliveryAttempt={} poisonPill={}",
                    event.eventId(), attempt, event.poisonPill());
            throw new IllegalStateException("Simulated processing failure for " + event.eventId());
        }
        processingService.process(event);
    }
}
