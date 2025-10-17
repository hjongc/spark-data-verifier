package com.dataverification.model;

/**
 * Verification mode for data comparison strategies.
 */
public enum VerificationMode {
    /**
     * Fast mode using SHA hashing + FULL OUTER JOIN.
     * Most efficient for Spark resources, faster execution.
     * Shows only if differences exist, not the exact values.
     */
    FAST,

    /**
     * Detailed mode using EXCEPT operator.
     * Shows exact values of differences.
     * Uses more Spark resources and slower due to sorting + comparison.
     */
    DETAILED
}
