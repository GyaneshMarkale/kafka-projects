package com.gyanesh.kafkalab.service;

import com.gyanesh.kafkalab.events.OrderEvent;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class EventProcessingServiceTest {
    private JdbcTemplate jdbcTemplate;
    private EventProcessingService service;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("""
                CREATE TABLE consumed_events (
                    event_id VARCHAR(100) PRIMARY KEY,
                    order_id VARCHAR(100) NOT NULL,
                    customer_id VARCHAR(150),
                    product_id VARCHAR(150),
                    quantity INTEGER NOT NULL,
                    correlation_id VARCHAR(150),
                    consumed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """);
        service = new EventProcessingService(jdbcTemplate);
    }

    @Test
    void firstDeliveryInsertsEvent() {
        assertThat(service.process(event("event-1")))
                .isEqualTo(EventProcessingService.ProcessingOutcome.PROCESSED);
        assertThat(service.countUniqueProcessedEvents()).isEqualTo(1);
    }

    @Test
    void replayWithSameEventIdDoesNotRepeatDatabaseSideEffect() {
        OrderEvent event = event("same-event-id");
        assertThat(service.process(event)).isEqualTo(EventProcessingService.ProcessingOutcome.PROCESSED);
        assertThat(service.process(event)).isEqualTo(EventProcessingService.ProcessingOutcome.DUPLICATE_IGNORED);
        assertThat(service.countUniqueProcessedEvents()).isEqualTo(1);
    }

    private static OrderEvent event(String eventId) {
        return new OrderEvent(eventId, "order-1", "customer-1", "product-1", 1,
                Instant.now().toString(), 0, false, "corr-1");
    }
}
