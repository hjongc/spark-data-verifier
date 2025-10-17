package com.dataverification;

import com.dataverification.config.ApplicationConfig;
import com.dataverification.model.VerificationMetrics;
import com.dataverification.model.VerificationMode;
import com.dataverification.service.DataVerificationService;
import com.dataverification.util.ConnectionManager;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main application entry point for data verification tool.
 *
 * This tool verifies data consistency between two databases during platform migration.
 * Originally used for Azure Synapse to AWS EMR migration.
 */
public class DataVerificationApplication {
    private static final Logger logger = LoggerFactory.getLogger(DataVerificationApplication.class);

    public static void main(String[] args) {
        logger.info("=== Spark Data Verification Tool ===");
        logger.info("Version: 1.0.0");

        try {
            // Parse command line arguments
            CommandLine cmd = parseCommandLine(args);

            // Load configuration
            ApplicationConfig config = ApplicationConfig.load();
            logger.info("Configuration loaded successfully");

            // Extract parameters
            String tableName = cmd.getOptionValue("t");
            String baseDatabase = cmd.getOptionValue("d");
            String targetDatabase = cmd.getOptionValue("a");
            String odate = cmd.getOptionValue("o");
            String mid = cmd.getOptionValue("m");
            String whereCondition = cmd.getOptionValue("w", "1=1");
            String excludeColumns = cmd.getOptionValue("e", "");
            VerificationMode mode = VerificationMode.valueOf(
                    cmd.getOptionValue("mode", "FAST").toUpperCase()
            );

            logger.info("Parameters:");
            logger.info("  Table: {}", tableName);
            logger.info("  Base DB: {}", baseDatabase);
            logger.info("  Target DB: {}", targetDatabase);
            logger.info("  Mode: {}", mode);
            logger.info("  Where: {}", whereCondition);
            logger.info("  Exclude columns: {}", excludeColumns.isEmpty() ? "(none)" : excludeColumns);
            logger.info("  Odate: {}", odate);
            logger.info("  MID: {}", mid);

            // Create verification service
            DataVerificationService service = new DataVerificationService(config);

            // Execute verification
            VerificationMetrics metrics = service.verifyTable(
                    baseDatabase, targetDatabase, tableName,
                    whereCondition, excludeColumns, mode, odate, mid
            );

            // Determine exit code based on results
            int exitCode = determineExitCode(metrics);
            logger.info("Verification completed with exit code: {}", exitCode);

            // Cleanup
            ConnectionManager.closeAll();
            System.exit(exitCode);

        } catch (ParseException e) {
            logger.error("Invalid command line arguments: {}", e.getMessage());
            printUsage();
            System.exit(1);
        } catch (Exception e) {
            logger.error("Verification failed with error", e);
            System.exit(2);
        }
    }

    /**
     * Parse command line arguments.
     */
    private static CommandLine parseCommandLine(String[] args) throws ParseException {
        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        // Validate required options
        if (!cmd.hasOption("t") || !cmd.hasOption("d") || !cmd.hasOption("a") ||
                !cmd.hasOption("o") || !cmd.hasOption("m")) {
            throw new ParseException("Missing required options");
        }

        return cmd;
    }

    /**
     * Create command line options.
     */
    private static Options createOptions() {
        Options options = new Options();

        options.addOption(Option.builder("t")
                .longOpt("tableName")
                .hasArg()
                .required()
                .desc("Table name to verify")
                .build());

        options.addOption(Option.builder("d")
                .longOpt("baseDatabase")
                .hasArg()
                .required()
                .desc("Base (source) database name")
                .build());

        options.addOption(Option.builder("a")
                .longOpt("targetDatabase")
                .hasArg()
                .required()
                .desc("Target (destination) database name")
                .build());

        options.addOption(Option.builder("o")
                .longOpt("odate")
                .hasArg()
                .required()
                .desc("Operation date (YYYYMMDD)")
                .build());

        options.addOption(Option.builder("m")
                .longOpt("mid")
                .hasArg()
                .required()
                .desc("Migration ID or batch identifier")
                .build());

        options.addOption(Option.builder("w")
                .longOpt("whereCondition")
                .hasArg()
                .desc("WHERE clause condition (default: 1=1)")
                .build());

        options.addOption(Option.builder("e")
                .longOpt("excludeColumns")
                .hasArg()
                .desc("Comma-separated list of columns to exclude from comparison")
                .build());

        options.addOption(Option.builder()
                .longOpt("mode")
                .hasArg()
                .desc("Verification mode: FAST (default) or DETAILED")
                .build());

        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Print this help message")
                .build());

        return options;
    }

    /**
     * Print usage information.
     */
    private static void printUsage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.setWidth(120);

        System.out.println("\nSpark Data Verification Tool - Enterprise-grade data verification");
        System.out.println("Used for verifying data consistency during platform migrations\n");

        formatter.printHelp("java -jar spark-data-verification.jar", createOptions(), true);

        System.out.println("\nVerification Modes:");
        System.out.println("  FAST     - SHA hash + FULL OUTER JOIN (faster, less resource intensive)");
        System.out.println("  DETAILED - EXCEPT operator (shows exact differences, more resources)\n");

        System.out.println("Examples:");
        System.out.println("  # Fast verification with default settings");
        System.out.println("  java -jar verification.jar -t users -d prod_db -a target_db -o 20250115 -m migration_001\n");

        System.out.println("  # Detailed verification with column exclusions");
        System.out.println("  java -jar verification.jar -t orders -d prod_db -a target_db -o 20250115 -m migration_001 \\");
        System.out.println("       --mode DETAILED -e \"updated_at,created_at\" -w \"order_date >= '2025-01-01'\"\n");

        System.out.println("Environment Variables:");
        System.out.println("  HIVE_JDBC_URL      - Hive/Spark JDBC connection URL");
        System.out.println("  HIVE_USERNAME      - Hive/Spark username");
        System.out.println("  HIVE_PASSWORD      - Hive/Spark password");
        System.out.println("  MYSQL_JDBC_URL     - MySQL result storage JDBC URL");
        System.out.println("  MYSQL_USERNAME     - MySQL username");
        System.out.println("  MYSQL_PASSWORD     - MySQL password");
    }

    /**
     * Determine exit code based on verification metrics.
     */
    private static int determineExitCode(VerificationMetrics metrics) {
        if (!metrics.getErrorMessages().isEmpty()) {
            return 2; // Errors occurred
        }

        if (metrics.getDifferencesFound() > 0) {
            return 1; // Differences found
        }

        return 0; // Success
    }
}
