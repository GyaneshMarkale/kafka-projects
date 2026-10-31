package com.gyanesh.kafkalab.events;

public record DemoReceipt(
        String demo,
        int messagesPublished,
        String eventId,
        int hotKeyMessages,
        int coldKeyMessages,
        String note) {
}
