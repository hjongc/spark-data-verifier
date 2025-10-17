package com.dataverification.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

/**
 * Application configuration loaded from application.yml and environment variables.
 * Environment variables take precedence over configuration file.
 */
public class ApplicationConfig {
    private DatabaseConfig hiveDatabase;
    private DatabaseConfig mysqlDatabase;
    private VerificationConfig verification;

    public static ApplicationConfig load() {
        ApplicationConfig config = new ApplicationConfig();

        // Load from YAML file
        try (InputStream input = ApplicationConfig.class.getClassLoader()
                .getResourceAsStream("application.yml")) {
            if (input != null) {
                Yaml yaml = new Yaml();
                Map<String, Object> data = yaml.load(input);
                config.loadFromYaml(data);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load application.yml", e);
        }

        // Override with environment variables
        config.overrideWithEnvironmentVariables();

        return config;
    }

    @SuppressWarnings("unchecked")
    private void loadFromYaml(Map<String, Object> data) {
        Map<String, Object> databases = (Map<String, Object>) data.get("database");

        if (databases != null) {
            Map<String, Object> hiveConfig = (Map<String, Object>) databases.get("hive");
            if (hiveConfig != null) {
                this.hiveDatabase = new DatabaseConfig();
                this.hiveDatabase.setJdbcUrl((String) hiveConfig.get("jdbcUrl"));
                this.hiveDatabase.setUsername((String) hiveConfig.get("username"));
                this.hiveDatabase.setPassword((String) hiveConfig.get("password"));
                this.hiveDatabase.setDriverClassName((String) hiveConfig.get("driverClassName"));
            }

            Map<String, Object> mysqlConfig = (Map<String, Object>) databases.get("mysql");
            if (mysqlConfig != null) {
                this.mysqlDatabase = new DatabaseConfig();
                this.mysqlDatabase.setJdbcUrl((String) mysqlConfig.get("jdbcUrl"));
                this.mysqlDatabase.setUsername((String) mysqlConfig.get("username"));
                this.mysqlDatabase.setPassword((String) mysqlConfig.get("password"));
                this.mysqlDatabase.setDriverClassName((String) mysqlConfig.get("driverClassName"));
            }
        }

        Map<String, Object> verificationConfig = (Map<String, Object>) data.get("verification");
        if (verificationConfig != null) {
            this.verification = new VerificationConfig();
            Object maxParallel = verificationConfig.get("maxParallelPartitions");
            if (maxParallel != null) {
                this.verification.setMaxParallelPartitions(((Number) maxParallel).intValue());
            }
            Object sampleLimit = verificationConfig.get("sampleLimit");
            if (sampleLimit != null) {
                this.verification.setSampleLimit(((Number) sampleLimit).intValue());
            }
            Object fetchSize = verificationConfig.get("jdbcFetchSize");
            if (fetchSize != null) {
                this.verification.setJdbcFetchSize(((Number) fetchSize).intValue());
            }
        }
    }

    private void overrideWithEnvironmentVariables() {
        // Hive database overrides
        String hiveUrl = System.getenv("HIVE_JDBC_URL");
        if (hiveUrl != null) {
            if (this.hiveDatabase == null) this.hiveDatabase = new DatabaseConfig();
            this.hiveDatabase.setJdbcUrl(hiveUrl);
        }

        String hiveUser = System.getenv("HIVE_USERNAME");
        if (hiveUser != null) {
            if (this.hiveDatabase == null) this.hiveDatabase = new DatabaseConfig();
            this.hiveDatabase.setUsername(hiveUser);
        }

        String hivePassword = System.getenv("HIVE_PASSWORD");
        if (hivePassword != null) {
            if (this.hiveDatabase == null) this.hiveDatabase = new DatabaseConfig();
            this.hiveDatabase.setPassword(hivePassword);
        }

        // MySQL database overrides
        String mysqlUrl = System.getenv("MYSQL_JDBC_URL");
        if (mysqlUrl != null) {
            if (this.mysqlDatabase == null) this.mysqlDatabase = new DatabaseConfig();
            this.mysqlDatabase.setJdbcUrl(mysqlUrl);
        }

        String mysqlUser = System.getenv("MYSQL_USERNAME");
        if (mysqlUser != null) {
            if (this.mysqlDatabase == null) this.mysqlDatabase = new DatabaseConfig();
            this.mysqlDatabase.setUsername(mysqlUser);
        }

        String mysqlPassword = System.getenv("MYSQL_PASSWORD");
        if (mysqlPassword != null) {
            if (this.mysqlDatabase == null) this.mysqlDatabase = new DatabaseConfig();
            this.mysqlDatabase.setPassword(mysqlPassword);
        }

        // Verification settings overrides
        String maxParallel = System.getenv("VERIFICATION_MAX_PARALLEL");
        if (maxParallel != null) {
            if (this.verification == null) this.verification = new VerificationConfig();
            this.verification.setMaxParallelPartitions(Integer.parseInt(maxParallel));
        }

        String sampleLimit = System.getenv("VERIFICATION_SAMPLE_LIMIT");
        if (sampleLimit != null) {
            if (this.verification == null) this.verification = new VerificationConfig();
            this.verification.setSampleLimit(Integer.parseInt(sampleLimit));
        }
    }

    public DatabaseConfig getHiveDatabase() {
        return hiveDatabase;
    }

    public void setHiveDatabase(DatabaseConfig hiveDatabase) {
        this.hiveDatabase = hiveDatabase;
    }

    public DatabaseConfig getMysqlDatabase() {
        return mysqlDatabase;
    }

    public void setMysqlDatabase(DatabaseConfig mysqlDatabase) {
        this.mysqlDatabase = mysqlDatabase;
    }

    public VerificationConfig getVerification() {
        return verification;
    }

    public void setVerification(VerificationConfig verification) {
        this.verification = verification;
    }
}
