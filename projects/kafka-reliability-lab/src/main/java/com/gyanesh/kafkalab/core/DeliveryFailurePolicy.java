package com.gyanesh.kafkalab.core;

/** Dependency-free policy for simulating transient and permanent consumer failures. */
public final class DeliveryFailurePolicy {
    private DeliveryFailurePolicy() {
    }

    public static boolean shouldFail(int deliveryAttempt, int failuresBeforeSuccess, boolean poisonPill) {
        if (deliveryAttempt < 1) {
            throw new IllegalArgumentException("deliveryAttempt must be at least 1");
        }
        if (failuresBeforeSuccess < 0) {
            throw new IllegalArgumentException("failuresBeforeSuccess cannot be negative");
        }
        return poisonPill || deliveryAttempt <= failuresBeforeSuccess;
    }
}
