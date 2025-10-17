package com.dataverification.util;

import com.dataverification.config.DatabaseConfig;
import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages database connections with connection pooling.
 */
public class ConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionManager.class);
    private static final Map<String, DataSource> dataSources = new ConcurrentHashMap<>();

    /**
     * Get a connection from the pool.
     */
    public static Connection getConnection(DatabaseConfig config) throws SQLException {
        String key = config.getJdbcUrl();
        DataSource dataSource = dataSources.computeIfAbsent(key, k -> createDataSource(config));
        return dataSource.getConnection();
    }

    /**
     * Create a pooled DataSource.
     */
    private static DataSource createDataSource(DatabaseConfig config) {
        logger.info("Creating connection pool for: {}", maskJdbcUrl(config.getJdbcUrl()));

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(config.getDriverClassName());
        dataSource.setUrl(config.getJdbcUrl());
        dataSource.setUsername(config.getUsername());
        dataSource.setPassword(config.getPassword());

        // Pool configuration
        dataSource.setInitialSize(config.getMinIdle());
        dataSource.setMaxTotal(config.getMaxPoolSize());
        dataSource.setMinIdle(config.getMinIdle());
        dataSource.setMaxWaitMillis(config.getConnectionTimeoutMs());

        // Connection validation
        dataSource.setTestOnBorrow(true);
        dataSource.setValidationQuery("SELECT 1");

        // Connection management
        dataSource.setRemoveAbandonedOnBorrow(true);
        dataSource.setRemoveAbandonedTimeout(300); // 5 minutes

        logger.info("Connection pool created successfully");
        return dataSource;
    }

    /**
     * Close all connection pools.
     */
    public static void closeAll() {
        logger.info("Closing all connection pools");
        dataSources.forEach((key, dataSource) -> {
            if (dataSource instanceof BasicDataSource) {
                try {
                    ((BasicDataSource) dataSource).close();
                    logger.info("Closed connection pool for: {}", maskJdbcUrl(key));
                } catch (SQLException e) {
                    logger.error("Error closing connection pool", e);
                }
            }
        });
        dataSources.clear();
    }

    /**
     * Mask sensitive information in JDBC URL for logging.
     */
    private static String maskJdbcUrl(String url) {
        if (url == null) return "null";
        return url.replaceAll("(?i)(password|pwd)=[^;&]*", "$1=***");
    }
}
