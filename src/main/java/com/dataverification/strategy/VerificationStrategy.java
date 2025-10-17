package com.dataverification.strategy;

import com.dataverification.model.TableMetadata;
import com.dataverification.model.VerificationResult;

import java.sql.Connection;
import java.util.List;

/**
 * Strategy interface for different verification approaches.
 */
public interface VerificationStrategy {
    /**
     * Verify data between base and target tables.
     *
     * @param connection database connection
     * @param baseDatabase base database name
     * @param targetDatabase target database name
     * @param tableName table name
     * @param columns columns to compare
     * @param whereCondition filter condition
     * @param sampleLimit limit for sample differences
     * @return verification result
     */
    VerificationResult verify(
            Connection connection,
            String baseDatabase,
            String targetDatabase,
            String tableName,
            List<String> columns,
            String whereCondition,
            int sampleLimit
    ) throws Exception;
}
