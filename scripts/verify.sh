#!/bin/bash
# Simple verification wrapper script

# Load environment variables if .env file exists
if [ -f "$(dirname "$0")/../.env" ]; then
    set -a
    source "$(dirname "$0")/../.env"
    set +a
fi

# Get JAR path
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_PATH="$SCRIPT_DIR/../target/spark-data-verifier-1.0.0.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo "Error: JAR file not found at $JAR_PATH"
    echo "Please run 'mvn clean package' first"
    exit 1
fi

# Run verification
java -Xmx4g -jar "$JAR_PATH" "$@"
