package com.gyanesh.kafkalab.service;

import com.gyanesh.kafkalab.events.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EventProcessingService {
    private static final Logger log = LoggerFactory.getLogger(EventProcessingService.class);
    private final JdbcTemplate jdbcTemplate;

    public EventProcessingService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * The unique event_id constraint is this demo's durable idempotency guard.
     * If the DB insert succeeds but offset commit does not, redelivery is a no-op.
     */
    @Transactional
    public ProcessingOutcome process(OrderEvent event) {
        try {
            jdbcTemplate.update("""
                    INSERT INTO consumed_events
                        (event_id, order_id, customer_id, product_id, quantity, correlation_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    event.eventId(), event.orderId(), event.customerId(), event.productId(),
                    event.quantity(), event.correlationId());
            log.info("EVENT_PROCESSED eventId={} orderId={} correlationId={}",
                    event.eventId(), event.orderId(), event.correlationId());
            return ProcessingOutcome.PROCESSED;
        } catch (DuplicateKeyException duplicate) {
            log.warn("DUPLICATE_IGNORED eventId={} orderId={}", event.eventId(), event.orderId());
            return ProcessingOutcome.DUPLICATE_IGNORED;
        }
    }

    public long countUniqueProcessedEvents() {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM consumed_events", Long.class);
        return count == null ? 0L : count;
    }

    public enum ProcessingOutcome {
        PROCESSED,
        DUPLICATE_IGNORED
    }
}
