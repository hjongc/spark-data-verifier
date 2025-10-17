package com.dataverification.service;

import com.dataverification.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * Service for retrieving table metadata.
 */
public class TableMetadataService {
    private static final Logger logger = LoggerFactory.getLogger(TableMetadataService.class);

    /**
     * Analyze table and retrieve metadata.
     */
    public TableMetadata analyzeTable(
            Connection connection,
            String database,
            String tableName,
            String excludeColumns
    ) throws SQLException {
        logger.info("Analyzing table: {}.{}", database, tableName);

        TableMetadata metadata = new TableMetadata();
        metadata.setDatabase(database);
        metadata.setTableName(tableName);

        // Get column information
        metadata.setColumns(getColumns(connection, database, tableName, excludeColumns));
        logger.info("Found {} columns to compare", metadata.getColumns().size());

        // Check if table is partitioned
        metadata.setPartitioned(isPartitioned(connection, database, tableName));
        logger.info("Table is {}partitioned", metadata.isPartitioned() ? "" : "not ");

        if (metadata.isPartitioned()) {
            // Get partition keys
            metadata.setPartitionKeys(getPartitionKeys(connection, database, tableName));
            logger.info("Partition keys: {}", metadata.getPartitionKeys());
        }

        return metadata;
    }

    /**
     * Get columns for comparison, excluding specified columns.
     */
    private List<String> getColumns(
            Connection connection,
            String database,
            String tableName,
            String excludeColumns
    ) throws SQLException {
        List<String> allColumns = new ArrayList<>();

        String sql = "SHOW COLUMNS FROM " + database + "." + tableName;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                allColumns.add(rs.getString(1));
            }
        }

        // Remove excluded columns
        if (excludeColumns != null && !excludeColumns.trim().isEmpty()) {
            String[] excludeArray = excludeColumns.toLowerCase().split(",");
            Set<String> excludeSet = new HashSet<>(Arrays.asList(excludeArray));

            allColumns.removeIf(col -> {
                boolean shouldExclude = excludeSet.contains(col.toLowerCase().trim());
                if (shouldExclude) {
                    logger.debug("Excluding column: {}", col);
                }
                return shouldExclude;
            });
        }

        if (allColumns.isEmpty()) {
            throw new IllegalStateException("No columns to compare after exclusions");
        }

        return allColumns;
    }

    /**
     * Check if table is partitioned.
     */
    private boolean isPartitioned(Connection connection, String database, String tableName) {
        String sql = "SHOW PARTITIONS " + database + "." + tableName;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            return rs.next(); // If any partition exists, table is partitioned

        } catch (SQLException e) {
            // SHOW PARTITIONS will fail for non-partitioned tables
            logger.debug("Table is not partitioned: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Get partition keys from the first partition.
     */
    private Map<String, String> getPartitionKeys(
            Connection connection,
            String database,
            String tableName
    ) throws SQLException {
        Map<String, String> partitionKeys = new HashMap<>();
        String sql = "SHOW PARTITIONS " + database + "." + tableName;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            if (rs.next()) {
                String firstPartition = rs.getString(1);
                // Parse partition string like "year=2023/month=01"
                String[] parts = firstPartition.split("/");

                for (int i = 0; i < parts.length; i++) {
                    String[] keyValue = parts[i].split("=");
                    if (keyValue.length == 2) {
                        partitionKeys.put(String.valueOf(i + 1), keyValue[0]);
                    }
                }
            }
        }

        return partitionKeys;
    }

    /**
     * Get all partition values matching the filter.
     * Uses dynamic queries to get actual partition values based on depth.
     */
    public List<String> getPartitions(
            Connection connection,
            String database,
            String tableName,
            String whereCondition
    ) throws SQLException {
        List<String> partitions = new ArrayList<>();

        // First, get partition structure
        Map<String, String> partitionKeys = getPartitionKeys(connection, database, tableName);

        if (partitionKeys.isEmpty()) {
            return partitions; // Not partitioned
        }

        try (Statement stmt = connection.createStatement()) {
            if (partitionKeys.containsKey("2")) {
                // 2-depth partitions: Get distinct combinations
                String firstKey = partitionKeys.get("1");
                String secondKey = partitionKeys.get("2");

                String sql = String.format(
                    "SELECT DISTINCT %s, %s FROM %s.%s WHERE %s",
                    firstKey, secondKey, database, tableName, whereCondition
                );

                logger.debug("Fetching 2-depth partitions: {}", sql);

                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        String firstValue = rs.getString(1);
                        String secondValue = rs.getString(2);
                        String partition = String.format("%s=%s/%s=%s",
                            firstKey, firstValue, secondKey, secondValue);
                        partitions.add(partition);
                    }
                }

            } else if (partitionKeys.containsKey("1")) {
                // 1-depth partition: Get distinct values
                String partitionKey = partitionKeys.get("1");

                String sql = String.format(
                    "SELECT DISTINCT %s FROM %s.%s WHERE %s",
                    partitionKey, database, tableName, whereCondition
                );

                logger.debug("Fetching 1-depth partitions: {}", sql);

                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        String value = rs.getString(1);
                        String partition = String.format("%s=%s", partitionKey, value);
                        partitions.add(partition);
                    }
                }
            }
        }

        logger.info("Found {} partitions for {}.{}", partitions.size(), database, tableName);
        return partitions;
    }

    /**
     * Build filter condition for a specific partition.
     */
    public String buildPartitionFilter(String partition, String baseFilter) {
        if (partition == null || "NO_PARTITION".equals(partition)) {
            return baseFilter;
        }

        // Parse partition string like "year=2023/month=01"
        String[] parts = partition.split("/");
        List<String> partitionConditions = new ArrayList<>();

        for (String part : parts) {
            String[] keyValue = part.split("=");
            if (keyValue.length == 2) {
                partitionConditions.add(String.format("%s='%s'", keyValue[0], keyValue[1]));
            }
        }

        String partitionFilter = String.join(" AND ", partitionConditions);
        return partitionFilter + " AND " + baseFilter;
    }
}
