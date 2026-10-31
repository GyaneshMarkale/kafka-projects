package com.gyanesh.kafkalab.service;

import com.gyanesh.kafkalab.core.DeliveryFailurePolicy;
import com.gyanesh.kafkalab.core.PartitionSkewPlanner;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class CoreReliabilityPolicyTest {
    @Test
    void transientFailuresStopAfterConfiguredAttemptCount() {
        assertThat(DeliveryFailurePolicy.shouldFail(1, 2, false)).isTrue();
        assertThat(DeliveryFailurePolicy.shouldFail(2, 2, false)).isTrue();
        assertThat(DeliveryFailurePolicy.shouldFail(3, 2, false)).isFalse();
    }

    @Test
    void poisonPillFailsAllAttempts() {
        assertThat(DeliveryFailurePolicy.shouldFail(1, 0, true)).isTrue();
        assertThat(DeliveryFailurePolicy.shouldFail(3, 0, true)).isTrue();
    }

    @Test
    void plannerUsesOneSharedHotKeyAndUniqueEventIds() {
        var plan = PartitionSkewPlanner.plan(1_000, 95);
        assertThat(plan.events()).hasSize(1_000);
        assertThat(plan.hotKeyCount()).isEqualTo(950);
        assertThat(plan.coldKeyCount()).isEqualTo(50);
        assertThat(plan.events().stream().filter(PartitionSkewPlanner.PlannedEvent::hotKey)
                .map(PartitionSkewPlanner.PlannedEvent::partitionKey).distinct()).hasSize(1);
        assertThat(plan.events().stream().map(PartitionSkewPlanner.PlannedEvent::eventId)
                .collect(Collectors.toCollection(HashSet::new))).hasSize(1_000);
    }

    @Test
    void invalidPlannerInputsAreRejected() {
        assertThatIllegalArgumentException().isThrownBy(() -> PartitionSkewPlanner.plan(0, 90));
        assertThatIllegalArgumentException().isThrownBy(() -> PartitionSkewPlanner.plan(100, 100));
    }
}
