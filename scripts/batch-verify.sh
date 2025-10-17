#!/bin/bash
# Batch verification script for multiple tables
# Usage: ./batch-verify.sh [base_db] [target_db] [odate] [mid]

set -e

# Configuration
BASE_DB="${1:-source_database}"
TARGET_DB="${2:-target_database}"
ODATE="${3:-$(date +%Y%m%d)}"
MID="${4:-migration_${ODATE}}"

# Tables to verify (customize this list)
TABLES=(
    "customer_info"
    "transaction_history"
    "product_catalog"
    "order_details"
    "user_preferences"
)

# Common exclusions (customize per your needs)
EXCLUDE_COLUMNS="oper_dt_hms,audit_dtm,updated_at"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERIFY_SCRIPT="$SCRIPT_DIR/verify.sh"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================="
echo "Batch Verification"
echo "========================================="
echo "Base Database: $BASE_DB"
echo "Target Database: $TARGET_DB"
echo "Operation Date: $ODATE"
echo "Migration ID: $MID"
echo "Tables: ${#TABLES[@]}"
echo "========================================="
echo

# Counters
TOTAL=${#TABLES[@]}
SUCCESS=0
FAILED=0
DIFFERENCES=0

# Track failed tables
FAILED_TABLES=()

# Process each table
for TABLE in "${TABLES[@]}"; do
    echo -e "${YELLOW}[$(date +%H:%M:%S)]${NC} Verifying $TABLE..."

    # Run FAST verification first
    if $VERIFY_SCRIPT \
        -t "$TABLE" \
        -d "$BASE_DB" \
        -a "$TARGET_DB" \
        -o "$ODATE" \
        -m "$MID" \
        --mode FAST \
        -e "$EXCLUDE_COLUMNS" \
        > /dev/null 2>&1; then

        echo -e "${GREEN}✓${NC} $TABLE verification passed (FAST mode)"
        ((SUCCESS++))

    else
        EXIT_CODE=$?

        if [ $EXIT_CODE -eq 1 ]; then
            # Differences found - run detailed verification
            echo -e "${YELLOW}⚠${NC} $TABLE has differences - running DETAILED mode..."

            if $VERIFY_SCRIPT \
                -t "$TABLE" \
                -d "$BASE_DB" \
                -a "$TARGET_DB" \
                -o "$ODATE" \
                -m "${MID}_detailed" \
                --mode DETAILED \
                -e "$EXCLUDE_COLUMNS" \
                > /dev/null 2>&1; then

                echo -e "${YELLOW}⚠${NC} $TABLE has differences (see MySQL results for details)"
                ((DIFFERENCES++))
            else
                echo -e "${RED}✗${NC} $TABLE DETAILED verification failed"
                FAILED_TABLES+=("$TABLE")
                ((FAILED++))
            fi

        else
            # Error occurred
            echo -e "${RED}✗${NC} $TABLE verification failed with error"
            FAILED_TABLES+=("$TABLE")
            ((FAILED++))
        fi
    fi

    echo
done

# Summary
echo "========================================="
echo "Verification Summary"
echo "========================================="
echo "Total tables: $TOTAL"
echo -e "${GREEN}Passed:${NC} $SUCCESS"
echo -e "${YELLOW}Differences found:${NC} $DIFFERENCES"
echo -e "${RED}Failed:${NC} $FAILED"
echo "========================================="

# List failed tables
if [ $FAILED -gt 0 ]; then
    echo -e "\n${RED}Failed Tables:${NC}"
    for TABLE in "${FAILED_TABLES[@]}"; do
        echo "  - $TABLE"
    done
    echo
    exit 1
fi

# Warn about differences
if [ $DIFFERENCES -gt 0 ]; then
    echo -e "\n${YELLOW}Note:${NC} $DIFFERENCES table(s) have differences."
    echo "Check MySQL verification_result table for details:"
    echo "  SELECT * FROM verification_result WHERE odate = '$ODATE' AND execution_status = 'NOT_OK';"
    echo
fi

echo -e "${GREEN}Batch verification completed successfully${NC}"
