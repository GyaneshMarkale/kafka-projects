package com.gyanesh.kafkalab.core;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Creates one hot key and many cold keys to demonstrate key-driven partition skew. */
public final class PartitionSkewPlanner {
    private PartitionSkewPlanner() {
    }

    public record PlannedEvent(String eventId, String partitionKey, boolean hotKey) {
    }

    public record Plan(List<PlannedEvent> events, int hotKeyCount, int coldKeyCount) {
        public Plan {
            events = List.copyOf(events);
        }
    }

    public static Plan plan(int eventCount, int hotPercent) {
        if (eventCount < 1 || eventCount > 10_000) {
            throw new IllegalArgumentException("eventCount must be between 1 and 10000");
        }
        if (hotPercent < 50 || hotPercent > 99) {
            throw new IllegalArgumentException("hotPercent must be between 50 and 99");
        }
        String runId = UUID.randomUUID().toString();
        int hotCount = eventCount * hotPercent / 100;
        String hotKey = "HOT-ORDER-" + runId;
        List<PlannedEvent> events = new ArrayList<>(eventCount);
        for (int i = 0; i < eventCount; i++) {
            boolean hot = i < hotCount;
            String key = hot ? hotKey : "COLD-ORDER-" + runId + "-" + i;
            events.add(new PlannedEvent(runId + "-event-" + i, key, hot));
        }
        return new Plan(events, hotCount, eventCount - hotCount);
    }
}
