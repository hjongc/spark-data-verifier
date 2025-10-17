package com.dataverification.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Helper class for retry logic with exponential backoff.
 */
public class RetryHelper {
    private static final Logger logger = LoggerFactory.getLogger(RetryHelper.class);

    /**
     * Execute a callable with retry logic.
     *
     * @param callable the operation to execute
     * @param maxAttempts maximum number of attempts
     * @param delayMs initial delay between retries in milliseconds
     * @param operationName name of the operation for logging
     * @return result of the callable
     */
    public static <T> T executeWithRetry(
            Callable<T> callable,
            int maxAttempts,
            long delayMs,
            String operationName
    ) throws Exception {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                logger.debug("Executing {} (attempt {}/{})", operationName, attempt, maxAttempts);
                return callable.call();
            } catch (Exception e) {
                lastException = e;
                logger.warn("Attempt {}/{} failed for {}: {}",
                        attempt, maxAttempts, operationName, e.getMessage());

                if (attempt < maxAttempts) {
                    long waitTime = delayMs * attempt; // Linear backoff
                    logger.info("Retrying {} in {}ms...", operationName, waitTime);
                    Thread.sleep(waitTime);
                } else {
                    logger.error("All {} attempts failed for {}", maxAttempts, operationName);
                }
            }
        }

        throw new RuntimeException(
                String.format("Operation '%s' failed after %d attempts", operationName, maxAttempts),
                lastException
        );
    }

    /**
     * Execute with default retry parameters (3 attempts, 1 second delay).
     */
    public static <T> T executeWithRetry(Callable<T> callable, String operationName) throws Exception {
        return executeWithRetry(callable, 3, 1000, operationName);
    }
}
