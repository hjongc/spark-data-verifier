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
 * Fast verification strategy using SHA hashing + FULL OUTER JOIN.
 * Most efficient for Spark resources - uses single JOIN operation.
 * Only indicates if differences exist, not the exact values.
 */
public class FastVerificationStrategy implements VerificationStrategy {
    private static final Logger logger = LoggerFactory.getLogger(FastVerificationStrategy.class);

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
        logger.info("Starting FAST verification (SHA + FULL OUTER JOIN) for {}.{}", baseDatabase, tableName);

        VerificationResult result = new VerificationResult();
        result.setTableName(tableName);
        result.setBaseDatabase(baseDatabase);
        result.setTargetDatabase(targetDatabase);
        result.setWhereCondition(whereCondition);
        result.setMode(VerificationMode.FAST);

        try (Statement stmt = connection.createStatement()) {
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

            // Step 2: SHA + FULL OUTER JOIN to find differences
            String joinQuery = buildFullOuterJoinQuery(
                    baseDatabase, targetDatabase, tableName, columns, whereCondition, sampleLimit
            );
            logger.debug("Executing FULL OUTER JOIN query: {}", joinQuery);

            ResultSet joinRs = stmt.executeQuery(joinQuery);
            List<String> sampleDifferences = ResultSetConverter.convertToStringList(joinRs);

            if (sampleDifferences.isEmpty()) {
                result.setStatus(VerificationStatus.OK);
                result.setMessage("All rows match (SHA comparison)");
                result.setDifferencesFound(0);
            } else {
                result.setStatus(VerificationStatus.NOT_OK);
                result.setMessage(String.format("Found %d sample differences", sampleDifferences.size()));
                result.setDifferencesFound(sampleDifferences.size());
                result.setSampleDifferences(sampleDifferences);
            }

            result.complete();
            logger.info("FAST verification completed - Status: {}, Differences: {}",
                    result.getStatus(), result.getDifferencesFound());

        } catch (Exception e) {
            logger.error("FAST verification failed", e);
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

    private String buildFullOuterJoinQuery(
            String baseDb, String targetDb, String table,
            List<String> columns, String where, int limit
    ) {
        // Build SHA hash expressions for both sides
        StringBuilder baseHash = new StringBuilder("sha(CONCAT_WS('',");
        StringBuilder targetHash = new StringBuilder("sha(CONCAT_WS('',");

        for (String column : columns) {
            baseHash.append("a.").append(column).append(",");
            targetHash.append("b.").append(column).append(",");
        }

        // Remove trailing comma
        baseHash.setLength(baseHash.length() - 1);
        targetHash.setLength(targetHash.length() - 1);

        baseHash.append("))");
        targetHash.append("))");

        String firstColumn = columns.get(0);

        // FULL OUTER JOIN query
        return String.format(
                "SELECT * FROM " +
                        "(SELECT * FROM %s.%s WHERE %s) a " +
                        "FULL OUTER JOIN " +
                        "(SELECT * FROM %s.%s WHERE %s) b " +
                        "ON %s = %s " +
                        "WHERE (a.%s IS NULL OR b.%s IS NULL) " +
                        "LIMIT %d",
                baseDb, table, where,
                targetDb, table, where,
                baseHash, targetHash,
                firstColumn, firstColumn,
                limit
        );
    }
}
