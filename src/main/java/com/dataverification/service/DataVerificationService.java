package com.dataverification.service;

import com.dataverification.config.ApplicationConfig;
import com.dataverification.config.DatabaseConfig;
import com.dataverification.model.*;
import com.dataverification.repository.ResultRepository;
import com.dataverification.strategy.DetailedVerificationStrategy;
import com.dataverification.strategy.FastVerificationStrategy;
import com.dataverification.strategy.VerificationStrategy;
import com.dataverification.util.ConnectionManager;
import com.dataverification.util.RetryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Main service for coordinating data verification operations.
 */
public class DataVerificationService {
    private static final Logger logger = LoggerFactory.getLogger(DataVerificationService.class);

    private final ApplicationConfig appConfig;
    private final TableMetadataService metadataService;
    private final ResultRepository resultRepository;

    public DataVerificationService(ApplicationConfig appConfig) {
        this.appConfig = appConfig;
        this.metadataService = new TableMetadataService();
        this.resultRepository = new ResultRepository(appConfig.getMysqlDatabase());
    }

    /**
     * Verify a table with the given parameters.
     */
    public VerificationMetrics verifyTable(
            String baseDatabase,
            String targetDatabase,
            String tableName,
            String whereCondition,
            String excludeColumns,
            VerificationMode mode,
            String odate,
            String mid
    ) throws Exception {
        logger.info("=== Starting Data Verification ===");
        logger.info("Table: {}.{}", baseDatabase, tableName);
        logger.info("Mode: {}", mode);
        logger.info("Where: {}", whereCondition);

        VerificationMetrics metrics = new VerificationMetrics();
        DatabaseConfig hiveConfig = appConfig.getHiveDatabase();

        // Ensure result table exists
        resultRepository.createTableIfNotExists();

        try (Connection connection = ConnectionManager.getConnection(hiveConfig)) {
            // Analyze table metadata
            TableMetadata metadata = metadataService.analyzeTable(
                    connection, baseDatabase, tableName, excludeColumns
            );

            logger.info("Table metadata: {}", metadata);

            // Choose verification strategy
            VerificationStrategy strategy = createStrategy(mode);

            if (metadata.isPartitioned()) {
                // Process partitioned table
                verifyPartitionedTable(
                        connection, metadata, baseDatabase, targetDatabase,
                        tableName, whereCondition, strategy, metrics, odate, mid
                );
            } else {
                // Process normal table
                verifyNormalTable(
                        connection, metadata.getColumns(), baseDatabase, targetDatabase,
                        tableName, whereCondition, strategy, metrics, odate, mid
                );
            }

        } catch (Exception e) {
            logger.error("Verification failed", e);
            metrics.addError(e.getMessage());
            throw e;
        }

        logMetrics(tableName, metrics, mode);
        return metrics;
    }

    /**
     * Verify a partitioned table with parallel processing.
     */
    private void verifyPartitionedTable(
            Connection connection,
            TableMetadata metadata,
            String baseDatabase,
            String targetDatabase,
            String tableName,
            String whereCondition,
            VerificationStrategy strategy,
            VerificationMetrics metrics,
            String odate,
            String mid
    ) throws Exception {
        List<String> partitions = metadataService.getPartitions(
                connection, baseDatabase, tableName, whereCondition
        );

        logger.info("Found {} partitions to process", partitions.size());

        int maxParallel = appConfig.getVerification().getMaxParallelPartitions();
        int threadPoolSize = Math.min(maxParallel, partitions.size());
        logger.info("Using thread pool size: {}", threadPoolSize);

        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        List<Future<VerificationResult>> futures = new ArrayList<>();

        // Submit partition verification tasks
        for (String partition : partitions) {
            Future<VerificationResult> future = executor.submit(() ->
                    verifyPartition(
                            metadata.getColumns(), baseDatabase, targetDatabase, tableName,
                            partition, whereCondition, strategy, odate, mid
                    )
            );
            futures.add(future);
        }

        // Collect results
        for (Future<VerificationResult> future : futures) {
            try {
                VerificationResult result = future.get();
                updateMetrics(metrics, result);

                if (result.getStatus() != VerificationStatus.OK) {
                    logger.warn("Partition {} verification: {}", result.getPartition(), result.getStatus());
                } else {
                    logger.info("Partition {} verification: OK ({}ms)",
                            result.getPartition(), result.getProcessingTimeMs());
                }

            } catch (Exception e) {
                logger.error("Partition verification failed", e);
                metrics.addError(e.getMessage());
            }
        }

        executor.shutdown();
        if (!executor.awaitTermination(30, TimeUnit.MINUTES)) {
            executor.shutdownNow();
            logger.warn("Some partition tasks did not complete within timeout");
        }
    }

    /**
     * Verify a single partition with retry logic.
     */
    private VerificationResult verifyPartition(
            List<String> columns,
            String baseDatabase,
            String targetDatabase,
            String tableName,
            String partition,
            String baseFilter,
            VerificationStrategy strategy,
            String odate,
            String mid
    ) {
        try {
            return RetryHelper.executeWithRetry(() -> {
                String partitionFilter = metadataService.buildPartitionFilter(partition, baseFilter);

                DatabaseConfig hiveConfig = appConfig.getHiveDatabase();
                try (Connection conn = ConnectionManager.getConnection(hiveConfig)) {
                    VerificationResult result = strategy.verify(
                            conn, baseDatabase, targetDatabase, tableName,
                            columns, partitionFilter, appConfig.getVerification().getSampleLimit()
                    );

                    result.setPartition(partition);
                    resultRepository.save(result, odate, mid);
                    return result;
                }
            }, appConfig.getVerification().getRetryAttempts(),
                    appConfig.getVerification().getRetryDelayMs(),
                    "Verify partition: " + partition);

        } catch (Exception e) {
            logger.error("Failed to verify partition: {}", partition, e);
            VerificationResult errorResult = new VerificationResult();
            errorResult.setTableName(tableName);
            errorResult.setPartition(partition);
            errorResult.setStatus(VerificationStatus.ERROR);
            errorResult.setMessage("Error: " + e.getMessage());
            errorResult.complete();
            return errorResult;
        }
    }

    /**
     * Verify a non-partitioned table.
     */
    private void verifyNormalTable(
            Connection connection,
            List<String> columns,
            String baseDatabase,
            String targetDatabase,
            String tableName,
            String whereCondition,
            VerificationStrategy strategy,
            VerificationMetrics metrics,
            String odate,
            String mid
    ) throws Exception {
        logger.info("Processing non-partitioned table");

        VerificationResult result = RetryHelper.executeWithRetry(() ->
                        strategy.verify(
                                connection, baseDatabase, targetDatabase, tableName,
                                columns, whereCondition, appConfig.getVerification().getSampleLimit()
                        ),
                appConfig.getVerification().getRetryAttempts(),
                appConfig.getVerification().getRetryDelayMs(),
                "Verify table: " + tableName
        );

        result.setPartition("NO_PARTITION");
        updateMetrics(metrics, result);
        resultRepository.save(result, odate, mid);

        logger.info("Table verification completed: {} ({}ms)",
                result.getStatus(), result.getProcessingTimeMs());
    }

    /**
     * Create verification strategy based on mode.
     */
    private VerificationStrategy createStrategy(VerificationMode mode) {
        switch (mode) {
            case DETAILED:
                logger.info("Using DETAILED verification strategy (EXCEPT)");
                return new DetailedVerificationStrategy();
            case FAST:
            default:
                logger.info("Using FAST verification strategy (SHA + FULL OUTER JOIN)");
                return new FastVerificationStrategy();
        }
    }

    /**
     * Update metrics with verification result.
     */
    private void updateMetrics(VerificationMetrics metrics, VerificationResult result) {
        metrics.addRowsProcessed(result.getBaseRowCount());
        metrics.addDifferences(result.getDifferencesFound());
        metrics.incrementPartitionsProcessed();
        metrics.recordPartitionTime(
                result.getPartition() != null ? result.getPartition() : "NO_PARTITION",
                result.getProcessingTimeMs()
        );

        if (result.getStatus() == VerificationStatus.ERROR) {
            metrics.addError(result.getMessage());
        }
    }

    /**
     * Log verification metrics.
     */
    private void logMetrics(String tableName, VerificationMetrics metrics, VerificationMode mode) {
        logger.info("=== Verification Complete for {} ===", tableName);
        logger.info("Mode: {}", mode);
        logger.info("Duration: {:.2f} seconds", metrics.getTotalDurationSeconds());
        logger.info("Partitions processed: {}", metrics.getPartitionsProcessed());
        logger.info("Total rows processed: {}", metrics.getTotalRowsProcessed());
        logger.info("Differences found: {}", metrics.getDifferencesFound());

        if (!metrics.getErrorMessages().isEmpty()) {
            logger.warn("Errors encountered: {}", metrics.getErrorMessages().size());
            metrics.getErrorMessages().forEach(msg -> logger.warn("  - {}", msg));
        }
    }
}
