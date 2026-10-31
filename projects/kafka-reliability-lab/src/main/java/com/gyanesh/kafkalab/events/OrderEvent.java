package com.gyanesh.kafkalab.events;

/** Immutable event schema used throughout this learning lab. */
public record OrderEvent(
        String eventId,
        String orderId,
        String customerId,
        String productId,
        int quantity,
        String occurredAt,
        int failuresBeforeSuccess,
        boolean poisonPill,
        String correlationId) {
}
