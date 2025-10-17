package com.dataverification.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata information for a table being verified.
 */
public class TableMetadata {
    private String tableName;
    private String database;
    private boolean partitioned;
    private List<String> columns;
    private Map<String, String> partitionKeys;
    private List<String> partitionValues;

    public TableMetadata() {
        this.columns = new ArrayList<>();
        this.partitionKeys = new HashMap<>();
        this.partitionValues = new ArrayList<>();
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public boolean isPartitioned() {
        return partitioned;
    }

    public void setPartitioned(boolean partitioned) {
        this.partitioned = partitioned;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public Map<String, String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(Map<String, String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    public void setPartitionValues(List<String> partitionValues) {
        this.partitionValues = partitionValues;
    }

    @Override
    public String toString() {
        return String.format("TableMetadata{table=%s.%s, partitioned=%s, columns=%d, partitions=%d}",
                database, tableName, partitioned, columns.size(), partitionValues.size());
    }
}
