# Deployment Guide

## Prerequisites

- Java 11+ installed
- Maven 3.6+ installed
- Access to Hive/Spark cluster via JDBC
- MySQL 8.0+ database for result storage

## Build

```bash
mvn clean package
```

This creates `target/spark-data-verifier-1.0.0.jar` with all dependencies.

## Configuration

### Option 1: Environment Variables (Recommended for Production)

```bash
export HIVE_JDBC_URL="jdbc:hive2://your-server:10009/default?kyuubi.engine.type=SPARK_SQL"
export HIVE_USERNAME="spark"
export HIVE_PASSWORD=""

export MYSQL_JDBC_URL="jdbc:mysql://your-mysql:3306/verification_db"
export MYSQL_USERNAME="verifier"
export MYSQL_PASSWORD="secure_password"
```

### Option 2: Configuration File

Edit `src/main/resources/application.yml` with your settings before building.

## Deployment

### Server Deployment

```bash
# Create deployment directory
mkdir -p /opt/data-verification
cd /opt/data-verification

# Copy JAR file
cp target/spark-data-verifier-1.0.0.jar .

# Create logs directory
mkdir -p logs

# Create environment file
cat > .env << 'EOF'
HIVE_JDBC_URL=jdbc:hive2://your-server:10009/default?kyuubi.engine.type=SPARK_SQL
HIVE_USERNAME=spark
HIVE_PASSWORD=
MYSQL_JDBC_URL=jdbc:mysql://your-mysql:3306/verification_db
MYSQL_USERNAME=verifier
MYSQL_PASSWORD=secure_password
EOF

# Make it secure
chmod 600 .env
```

### Create Run Script

```bash
cat > verify.sh << 'EOF'
#!/bin/bash
set -a
source /opt/data-verification/.env
set +a

java -Xmx4g -jar /opt/data-verification/spark-data-verifier-1.0.0.jar "$@"
EOF

chmod +x verify.sh
```

## Usage

### Single Table Verification

```bash
./verify.sh \
  -t users \
  -d source_db \
  -a target_db \
  -o 20250115 \
  -m migration_batch_001
```

### With Filters and Exclusions

```bash
./verify.sh \
  -t transactions \
  -d source_db \
  -a target_db \
  -o 20250115 \
  -m migration_batch_001 \
  --mode DETAILED \
  -e "updated_at,created_at" \
  -w "date >= '2025-01-01'"
```

### Batch Verification Script

```bash
cat > batch_verify.sh << 'EOF'
#!/bin/bash
ODATE=$(date +%Y%m%d)
MID="migration_${ODATE}"

TABLES=(
  "customer_info"
  "order_history"
  "product_catalog"
)

for TABLE in "${TABLES[@]}"; do
  echo "Verifying $TABLE..."
  ./verify.sh -t "$TABLE" -d source_db -a target_db -o "$ODATE" -m "$MID"

  if [ $? -ne 0 ]; then
    echo "ERROR: $TABLE verification failed"
    exit 1
  fi
done
EOF

chmod +x batch_verify.sh
```

## Integration with Backend API

As mentioned, this tool is typically accessed by a backend API server that:
1. Receives user verification requests
2. Manages queue and execution
3. Calls this JAR programmatically or via shell

### Programmatic Integration (Java)

```java
ProcessBuilder pb = new ProcessBuilder(
    "java", "-jar", "/opt/data-verification/spark-data-verifier-1.0.0.jar",
    "-t", tableName,
    "-d", baseDatabase,
    "-a", targetDatabase,
    "-o", odate,
    "-m", mid
);

// Set environment variables
Map<String, String> env = pb.environment();
env.put("HIVE_JDBC_URL", hiveUrl);
env.put("MYSQL_JDBC_URL", mysqlUrl);

Process process = pb.start();
int exitCode = process.waitFor();
```

### REST API Example (Backend Service)

```java
@PostMapping("/api/verification/submit")
public ResponseEntity<?> submitVerification(@RequestBody VerificationRequest request) {
    // Queue the verification job
    String jobId = verificationService.submitJob(request);
    return ResponseEntity.ok(Map.of("jobId", jobId));
}

@GetMapping("/api/verification/{jobId}/status")
public ResponseEntity<?> getStatus(@PathVariable String jobId) {
    // Query MySQL verification_result table
    VerificationStatus status = verificationService.getJobStatus(jobId);
    return ResponseEntity.ok(status);
}
```

## Monitoring

### Check Logs

```bash
tail -f /opt/data-verification/logs/verification.log
```

### Query Results

```sql
-- Recent verifications
SELECT
    table_name,
    execution_status,
    processing_time_ms,
    differences_found,
    created_at
FROM verification_result
WHERE odate = '20250115'
ORDER BY created_at DESC;

-- Failed verifications
SELECT
    table_name,
    partition_key,
    sample_data
FROM verification_result
WHERE execution_status = 'NOT_OK'
  AND odate = '20250115';
```

## Performance Tuning

### JVM Settings

```bash
# Increase heap for large tables
java -Xmx8g -Xms4g -jar spark-data-verifier-1.0.0.jar ...

# Enable G1GC for better throughput
java -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Xmx8g -jar ...
```

### Configuration Tuning

```yaml
verification:
  # Match your Spark executor count
  maxParallelPartitions: 200  # If you have 200 executors

  # Increase for larger result sets
  jdbcFetchSize: 50000
```

## Troubleshooting

### Connection Timeout

Increase connection timeout in application.yml:
```yaml
database:
  hive:
    connectionTimeoutMs: 60000  # 60 seconds
```

### Out of Memory

- Increase JVM heap: `-Xmx8g`
- Reduce parallel partitions
- Reduce JDBC fetch size

### Slow Performance

- Use FAST mode first, then DETAILED only if needed
- Exclude timestamp columns that change during migration
- Add appropriate WHERE conditions to limit data volume

## Security

### Protect Credentials

```bash
# Secure the .env file
chmod 600 .env
chown verifier:verifier .env

# Use secrets management in production
# AWS: Use Secrets Manager
# K8s: Use Secrets
```

### Network Security

- Ensure firewall rules allow:
  - Connection to Hive/Spark (port 10009)
  - Connection to MySQL (port 3306)
- Use VPN or private networks for sensitive data

## Backup and Recovery

### Backup MySQL Results

```bash
mysqldump -u verifier -p verification_db verification_result > backup_$(date +%Y%m%d).sql
```

### Archive Logs

```bash
tar -czf logs_$(date +%Y%m%d).tar.gz logs/
```
