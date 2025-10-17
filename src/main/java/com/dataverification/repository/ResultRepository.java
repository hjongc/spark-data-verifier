package com.dataverification.repository;

import com.dataverification.config.DatabaseConfig;
import com.dataverification.model.VerificationResult;
import com.dataverification.util.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;

/**
 * Repository for saving verification results to MySQL.
 */
public class ResultRepository {
    private static final Logger logger = LoggerFactory.getLogger(ResultRepository.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final DatabaseConfig mysqlConfig;

    public ResultRepository(DatabaseConfig mysqlConfig) {
        this.mysqlConfig = mysqlConfig;
    }

    /**
     * Save verification result to database.
     */
    public void save(VerificationResult result, String odate, String mid) {
        String sql = "INSERT INTO verification_result " +
                "(table_name, base_database_name, target_database_name, partition_key, " +
                "execution_status, sample_data, start_time, end_time, processing_time_ms, " +
                "base_row_count, target_row_count, differences_found, where_condition, " +
                "verification_mode, odate, mid) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = ConnectionManager.getConnection(mysqlConfig);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, result.getTableName());
            pstmt.setString(2, result.getBaseDatabase());
            pstmt.setString(3, result.getTargetDatabase());
            pstmt.setString(4, result.getPartition() != null ? result.getPartition() : "NO_PARTITION");
            pstmt.setString(5, result.getStatus().name());
            pstmt.setString(6, formatSampleData(result));
            pstmt.setString(7, result.getStartTime().format(FORMATTER));
            pstmt.setString(8, result.getEndTime() != null ? result.getEndTime().format(FORMATTER) : null);
            pstmt.setLong(9, result.getProcessingTimeMs());
            pstmt.setLong(10, result.getBaseRowCount());
            pstmt.setLong(11, result.getTargetRowCount());
            pstmt.setLong(12, result.getDifferencesFound());
            pstmt.setString(13, result.getWhereCondition());
            pstmt.setString(14, result.getMode() != null ? result.getMode().name() : "FAST");
            pstmt.setString(15, odate);
            pstmt.setString(16, mid);

            int rowsAffected = pstmt.executeUpdate();
            logger.debug("Saved verification result to database: {} rows affected", rowsAffected);

        } catch (SQLException e) {
            logger.error("Failed to save verification result to database", e);
            // Don't throw - we don't want to fail the verification just because logging failed
        }
    }

    private String formatSampleData(VerificationResult result) {
        if (result.getSampleDifferences() == null || result.getSampleDifferences().isEmpty()) {
            return result.getMessage();
        }

        StringBuilder sb = new StringBuilder();
        sb.append(result.getMessage()).append("\n\n");
        sb.append("Sample differences:\n");

        int count = 0;
        for (String diff : result.getSampleDifferences()) {
            if (count++ >= 10) { // Limit to 10 samples for database storage
                sb.append("... (truncated)");
                break;
            }
            sb.append(diff).append("\n");
        }

        return sb.toString();
    }

    /**
     * Create verification_result table if it doesn't exist.
     */
    public void createTableIfNotExists() {
        String sql = "CREATE TABLE IF NOT EXISTS verification_result (" +
                "id BIGINT AUTO_INCREMENT PRIMARY KEY, " +
                "table_name VARCHAR(255) NOT NULL, " +
                "base_database_name VARCHAR(255) NOT NULL, " +
                "target_database_name VARCHAR(255) NOT NULL, " +
                "partition_key VARCHAR(500), " +
                "execution_status VARCHAR(50) NOT NULL, " +
                "sample_data TEXT, " +
                "start_time DATETIME NOT NULL, " +
                "end_time DATETIME, " +
                "processing_time_ms BIGINT, " +
                "base_row_count BIGINT, " +
                "target_row_count BIGINT, " +
                "differences_found BIGINT, " +
                "where_condition TEXT, " +
                "verification_mode VARCHAR(50), " +
                "odate VARCHAR(50), " +
                "mid VARCHAR(100), " +
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "INDEX idx_table_odate (table_name, odate), " +
                "INDEX idx_status (execution_status), " +
                "INDEX idx_created (created_at)" +
                ")";

        try (Connection conn = ConnectionManager.getConnection(mysqlConfig);
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.executeUpdate();
            logger.info("Verified verification_result table exists");

        } catch (SQLException e) {
            logger.warn("Could not create verification_result table: {}", e.getMessage());
        }
    }
}
