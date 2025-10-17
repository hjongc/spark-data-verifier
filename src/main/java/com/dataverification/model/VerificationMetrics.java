package com.dataverification.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics collected during verification execution.
 */
public class VerificationMetrics {
    private final long startTime;
    private final AtomicLong totalRowsProcessed;
    private final AtomicLong differencesFound;
    private final AtomicLong partitionsProcessed;
    private final List<String> errorMessages;
    private final Map<String, Long> partitionProcessingTimes;

    public VerificationMetrics() {
        this.startTime = System.currentTimeMillis();
        this.totalRowsProcessed = new AtomicLong(0);
        this.differencesFound = new AtomicLong(0);
        this.partitionsProcessed = new AtomicLong(0);
        this.errorMessages = new ArrayList<>();
        this.partitionProcessingTimes = new HashMap<>();
    }

    public void addRowsProcessed(long rows) {
        totalRowsProcessed.addAndGet(rows);
    }

    public void addDifferences(long differences) {
        differencesFound.addAndGet(differences);
    }

    public void incrementPartitionsProcessed() {
        partitionsProcessed.incrementAndGet();
    }

    public synchronized void addError(String error) {
        errorMessages.add(error);
    }

    public synchronized void recordPartitionTime(String partition, long timeMs) {
        partitionProcessingTimes.put(partition, timeMs);
    }

    public long getStartTime() {
        return startTime;
    }

    public long getTotalRowsProcessed() {
        return totalRowsProcessed.get();
    }

    public long getDifferencesFound() {
        return differencesFound.get();
    }

    public long getPartitionsProcessed() {
        return partitionsProcessed.get();
    }

    public List<String> getErrorMessages() {
        return new ArrayList<>(errorMessages);
    }

    public Map<String, Long> getPartitionProcessingTimes() {
        return new HashMap<>(partitionProcessingTimes);
    }

    public long getTotalDurationMs() {
        return System.currentTimeMillis() - startTime;
    }

    public double getTotalDurationSeconds() {
        return getTotalDurationMs() / 1000.0;
    }
}
