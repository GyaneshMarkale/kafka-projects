package com.gyanesh.kafkalab.consumer;

import com.gyanesh.kafkalab.config.KafkaTopicConfig;
import com.gyanesh.kafkalab.events.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class DeadLetterObserver {
    private static final Logger log = LoggerFactory.getLogger(DeadLetterObserver.class);

    @KafkaListener(topics = KafkaTopicConfig.DLT_TOPIC, groupId = "order-processing-dlt-observer")
    public void observe(
            OrderEvent event,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String failureMessage) {
        log.error("DLT_RECEIVED eventId={} orderId={} failureMessage={}",
                event.eventId(), event.orderId(), failureMessage);
    }
}
