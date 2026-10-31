package com.gyanesh.kafkalab.events;

public record PublishReceipt(
        String eventId,
        String orderId,
        String topic,
        int partition,
        long offset,
        String status,
        String note) {
}
