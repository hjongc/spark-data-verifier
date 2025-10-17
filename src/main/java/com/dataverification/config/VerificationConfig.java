package com.dataverification.config;

/**
 * Verification-specific configuration.
 */
public class VerificationConfig {
    private int maxParallelPartitions = 100;
    private int sampleLimit = 5;
    private int jdbcFetchSize = 20000;
    private int retryAttempts = 3;
    private long retryDelayMs = 1000;

    public int getMaxParallelPartitions() {
        return maxParallelPartitions;
    }

    public void setMaxParallelPartitions(int maxParallelPartitions) {
        this.maxParallelPartitions = maxParallelPartitions;
    }

    public int getSampleLimit() {
        return sampleLimit;
    }

    public void setSampleLimit(int sampleLimit) {
        this.sampleLimit = sampleLimit;
    }

    public int getJdbcFetchSize() {
        return jdbcFetchSize;
    }

    public void setJdbcFetchSize(int jdbcFetchSize) {
        this.jdbcFetchSize = jdbcFetchSize;
    }

    public int getRetryAttempts() {
        return retryAttempts;
    }

    public void setRetryAttempts(int retryAttempts) {
        this.retryAttempts = retryAttempts;
    }

    public long getRetryDelayMs() {
        return retryDelayMs;
    }

    public void setRetryDelayMs(long retryDelayMs) {
        this.retryDelayMs = retryDelayMs;
    }
}
