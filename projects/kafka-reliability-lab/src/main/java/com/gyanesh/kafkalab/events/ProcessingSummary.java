package com.gyanesh.kafkalab.events;

public record ProcessingSummary(long uniqueEventsProcessed, String explanation) {
}
