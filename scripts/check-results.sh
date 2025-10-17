#!/bin/bash
# Check verification results from MySQL
# Usage: ./check-results.sh [odate]

ODATE="${1:-$(date +%Y%m%d)}"

# Load MySQL credentials from environment
if [ -f "$(dirname "$0")/../.env" ]; then
    set -a
    source "$(dirname "$0")/../.env"
    set +a
fi

# Extract MySQL connection info
MYSQL_HOST=$(echo "$MYSQL_JDBC_URL" | sed -n 's|.*//\([^:]*\).*|\1|p')
MYSQL_PORT=$(echo "$MYSQL_JDBC_URL" | sed -n 's|.*:\([0-9]*\)/.*|\1|p')
MYSQL_DB=$(echo "$MYSQL_JDBC_URL" | sed -n 's|.*/\([^?]*\).*|\1|p')

if [ -z "$MYSQL_HOST" ] || [ -z "$MYSQL_USERNAME" ]; then
    echo "Error: MySQL configuration not found"
    echo "Please set MYSQL_JDBC_URL and MYSQL_USERNAME in .env file"
    exit 1
fi

echo "Checking verification results for odate: $ODATE"
echo "=========================================="
echo

# Summary query
mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USERNAME" -p"$MYSQL_PASSWORD" "$MYSQL_DB" <<EOF
SELECT
    execution_status,
    COUNT(*) as count,
    ROUND(AVG(processing_time_ms)/1000, 2) as avg_time_sec
FROM verification_result
WHERE odate = '$ODATE'
GROUP BY execution_status;
EOF

echo
echo "=========================================="
echo "Tables with differences:"
echo "=========================================="
echo

# Differences query
mysql -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USERNAME" -p"$MYSQL_PASSWORD" "$MYSQL_DB" <<EOF
SELECT
    table_name,
    partition_key,
    base_row_count,
    target_row_count,
    differences_found,
    LEFT(sample_data, 100) as sample
FROM verification_result
WHERE odate = '$ODATE'
  AND execution_status = 'NOT_OK'
ORDER BY created_at DESC
LIMIT 10;
EOF
