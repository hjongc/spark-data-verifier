package com.dataverification.strategy;

import com.dataverification.model.VerificationMode;
import com.dataverification.model.VerificationResult;
import com.dataverification.model.VerificationStatus;
import com.dataverification.util.ResultSetConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

/**
 * Detailed verification strategy using EXCEPT operator.
 * Shows exact values of differences but uses more resources.
 * Requires sorting and full comparison of all columns.
 */
public class DetailedVerificationStrategy implements VerificationStrategy {
    private static final Logger logger = LoggerFactory.getLogger(DetailedVerificationStrategy.class);

    @Override
    public VerificationResult verify(
            Connection connection,
            String baseDatabase,
            String targetDatabase,
            String tableName,
            List<String> columns,
            String whereCondition,
            int sampleLimit
    ) throws Exception {
        logger.info("Starting DETAILED verification (EXCEPT) for {}.{}", baseDatabase, tableName);

        VerificationResult result = new VerificationResult();
        result.setTableName(tableName);
        result.setBaseDatabase(baseDatabase);
        result.setTargetDatabase(targetDatabase);
        result.setWhereCondition(whereCondition);
        result.setMode(VerificationMode.DETAILED);

        try (Statement stmt = connection.createStatement()) {
            // Enable AQE for EXCEPT optimization
            stmt.execute("SET spark.sql.adaptive.enabled=true");
            stmt.execute("SET spark.sql.adaptive.coalescePartitions.enabled=true");

            // Step 1: Count comparison
            String countQuery = buildCountQuery(baseDatabase, targetDatabase, tableName, whereCondition);
            logger.debug("Executing count query: {}", countQuery);

            ResultSet countRs = stmt.executeQuery(countQuery);
            if (countRs.next()) {
                long baseCount = countRs.getLong("base_count");
                long targetCount = countRs.getLong("target_count");
                result.setBaseRowCount(baseCount);
                result.setTargetRowCount(targetCount);

                logger.info("Row counts - Base: {}, Target: {}", baseCount, targetCount);

                if (baseCount != targetCount) {
                    result.setStatus(VerificationStatus.NOT_OK);
                    result.setMessage("Row count mismatch");
                    result.setDifferencesFound(Math.abs(baseCount - targetCount));
                    result.complete();
                    return result;
                }

                if (baseCount == 0) {
                    result.setStatus(VerificationStatus.OK);
                    result.setMessage("Both tables are empty");
                    result.complete();
                    return result;
                }
            }

            // Step 2: EXCEPT to find differences (base - target)
            String exceptQuery = buildExceptQuery(
                    baseDatabase, targetDatabase, tableName, columns, whereCondition, sampleLimit
            );
            logger.debug("Executing EXCEPT query (base - target): {}", exceptQuery);

            ResultSet exceptRs = stmt.executeQuery(exceptQuery);
            List<String> baseDifferences = ResultSetConverter.convertToStringList(exceptRs);

            // Step 3: Reverse EXCEPT (target - base) for symmetric check
            String reverseExceptQuery = buildExceptQuery(
                    targetDatabase, baseDatabase, tableName, columns, whereCondition, sampleLimit
            );
            logger.debug("Executing reverse EXCEPT query (target - base): {}", reverseExceptQuery);

            ResultSet reverseRs = stmt.executeQuery(reverseExceptQuery);
            List<String> targetDifferences = ResultSetConverter.convertToStringList(reverseRs);

            // Combine results
            if (baseDifferences.isEmpty() && targetDifferences.isEmpty()) {
                result.setStatus(VerificationStatus.OK);
                result.setMessage("All rows are identical");
                result.setDifferencesFound(0);
            } else {
                result.setStatus(VerificationStatus.NOT_OK);
                int totalDifferences = baseDifferences.size() + targetDifferences.size();
                result.setMessage(String.format("Found %d differences (%d in base, %d in target)",
                        totalDifferences, baseDifferences.size(), targetDifferences.size()));
                result.setDifferencesFound(totalDifferences);

                // Combine sample differences with labels
                baseDifferences.replaceAll(s -> "[BASE ONLY] " + s);
                targetDifferences.replaceAll(s -> "[TARGET ONLY] " + s);
                baseDifferences.addAll(targetDifferences);
                result.setSampleDifferences(baseDifferences);
            }

            result.complete();
            logger.info("DETAILED verification completed - Status: {}, Differences: {}",
                    result.getStatus(), result.getDifferencesFound());

        } catch (Exception e) {
            logger.error("DETAILED verification failed", e);
            result.setStatus(VerificationStatus.ERROR);
            result.setMessage("Verification error: " + e.getMessage());
            result.complete();
            throw e;
        }

        return result;
    }

    private String buildCountQuery(String baseDb, String targetDb, String table, String where) {
        return String.format(
                "SELECT " +
                        "  (SELECT COUNT(*) FROM %s.%s WHERE %s) as base_count, " +
                        "  (SELECT COUNT(*) FROM %s.%s WHERE %s) as target_count",
                baseDb, table, where,
                targetDb, table, where
        );
    }

    private String buildExceptQuery(
            String sourceDb, String exceptDb, String table,
            List<String> columns, String where, int limit
    ) {
        String columnList = String.join(", ", columns);
        String orderBy = columns.get(0); // Use first column for ordering

        return String.format(
                "SELECT %s FROM %s.%s WHERE %s " +
                        "EXCEPT " +
                        "SELECT %s FROM %s.%s WHERE %s " +
                        "ORDER BY %s LIMIT %d",
                columnList, sourceDb, table, where,
                columnList, exceptDb, table, where,
                orderBy, limit
        );
    }
}
