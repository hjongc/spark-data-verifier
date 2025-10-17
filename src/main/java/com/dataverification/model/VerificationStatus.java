package com.dataverification.model;

/**
 * Status of verification execution.
 */
public enum VerificationStatus {
    /**
     * Verification passed - data matches perfectly.
     */
    OK,

    /**
     * Verification failed - differences found.
     */
    NOT_OK,

    /**
     * Verification encountered an error.
     */
    ERROR,

    /**
     * Verification completed with warnings.
     */
    WARNING
}
