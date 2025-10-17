package com.dataverification.model;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Result of a table or partition verification.
 */
public class VerificationResult {
    private String tableName;
    private String baseDatabase;
    private String targetDatabase;
    private String partition;
    private VerificationStatus status;
    private String message;
    private long baseRowCount;
    private long targetRowCount;
    private long differencesFound;
    private List<String> sampleDifferences;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private long processingTimeMs;
    private String whereCondition;
    private VerificationMode mode;

    public VerificationResult() {
        this.sampleDifferences = new ArrayList<>();
        this.startTime = LocalDateTime.now();
    }

    public void complete() {
        this.endTime = LocalDateTime.now();
        this.processingTimeMs = java.time.Duration.between(startTime, endTime).toMillis();
    }

    // Getters and Setters
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getBaseDatabase() {
        return baseDatabase;
    }

    public void setBaseDatabase(String baseDatabase) {
        this.baseDatabase = baseDatabase;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public VerificationStatus getStatus() {
        return status;
    }

    public void setStatus(VerificationStatus status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getBaseRowCount() {
        return baseRowCount;
    }

    public void setBaseRowCount(long baseRowCount) {
        this.baseRowCount = baseRowCount;
    }

    public long getTargetRowCount() {
        return targetRowCount;
    }

    public void setTargetRowCount(long targetRowCount) {
        this.targetRowCount = targetRowCount;
    }

    public long getDifferencesFound() {
        return differencesFound;
    }

    public void setDifferencesFound(long differencesFound) {
        this.differencesFound = differencesFound;
    }

    public List<String> getSampleDifferences() {
        return sampleDifferences;
    }

    public void setSampleDifferences(List<String> sampleDifferences) {
        this.sampleDifferences = sampleDifferences;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public long getProcessingTimeMs() {
        return processingTimeMs;
    }

    public void setProcessingTimeMs(long processingTimeMs) {
        this.processingTimeMs = processingTimeMs;
    }

    public String getWhereCondition() {
        return whereCondition;
    }

    public void setWhereCondition(String whereCondition) {
        this.whereCondition = whereCondition;
    }

    public VerificationMode getMode() {
        return mode;
    }

    public void setMode(VerificationMode mode) {
        this.mode = mode;
    }

    @Override
    public String toString() {
        return String.format("VerificationResult{table=%s, partition=%s, status=%s, " +
                        "baseCount=%d, targetCount=%d, differences=%d, time=%dms, mode=%s}",
                tableName, partition, status, baseRowCount, targetRowCount,
                differencesFound, processingTimeMs, mode);
    }
}
