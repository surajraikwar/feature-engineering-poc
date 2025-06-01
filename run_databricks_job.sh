#!/bin/bash

# Exit on error and print commands as they are executed
set -eo pipefail

# Function to print usage information
print_usage() {
  echo "Usage: $0 [JOB_CONFIG] [SOURCE_CATALOG]"
  echo ""
  echo "Run a feature engineering job on Databricks"
  echo ""
  echo "Arguments:"
  echo "  JOB_CONFIG     Path to job configuration YAML file (default: src/main/resources/configs/jobs/generate_transaction_features_job.yaml)"
  echo "  SOURCE_CATALOG Path to directory containing source definitions (default: src/main/resources/source_catalog)"
  echo ""
  echo "Environment variables (can be set in .env file or in the environment):"
  echo "  DATABRICKS_SERVER_HOSTNAME  Databricks workspace URL (e.g., dbc-xxx.cloud.databricks.com)"
  echo "  DATABRICKS_HTTP_PATH       Databricks HTTP path (e.g., /sql/protocolv1/o/...)"
  echo "  DATABRICKS_TOKEN           Databricks access token"
  echo "  DATABRICKS_CLUSTER_ID      Databricks cluster ID"
  echo "  DATABRICKS_CATALOG         Databricks catalog name"
  echo "  DATABRICKS_SCHEMA          Databricks schema name"
  echo ""
  echo "Example:"
  echo "  $0 src/main/resources/configs/jobs/my_job.yaml src/main/resources/source_catalog"
}

# Print help if requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  print_usage
  exit 0
fi

# Log function
log() {
  echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1"
}

# Error logging function
error_exit() {
  log "ERROR: $1" >&2
  exit 1
}

# Check if required commands are available
for cmd in sbt spark-submit; do
  if ! command -v $cmd &> /dev/null; then
    error_exit "Required command '$cmd' not found. Please install it and try again."
  fi
done

# Load environment variables from .env file if it exists
if [ -f ".env" ]; then
  log "Loading environment variables from .env file"
  # This approach handles comments and empty lines properly
  while IFS= read -r line; do
    # Skip empty lines and comments
    if [ -n "$line" ] && ! [[ "$line" =~ ^[[:space:]]*# ]]; then
      # Export the variable if it's a valid assignment
      if [[ "$line" =~ ^[a-zA-Z_][a-zA-Z0-9_]*= ]]; then
        export "$line"
      else
        log "Warning: Skipping invalid line in .env: $line"
      fi
    fi
  done < ".env"
else
  log "No .env file found, using environment variables from the current shell"
fi

# Required environment variables
REQUIRED_VARS=(
  "DATABRICKS_SERVER_HOSTNAME"
  "DATABRICKS_HTTP_PATH"
  "DATABRICKS_TOKEN"
  "DATABRICKS_CLUSTER_ID"
  "DATABRICKS_CATALOG"
  "DATABRICKS_SCHEMA"
)

# Check if required environment variables are set
MISSING_VARS=0
for var in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!var}" ]; then
    log "Error: Required environment variable $var is not set"
    MISSING_VARS=1
  fi
done

if [ $MISSING_VARS -ne 0 ]; then
  echo ""
  print_usage
  exit 1
fi

# Set default values if not provided as arguments
JOB_CONFIG="${1:-src/main/resources/configs/jobs/generate_transaction_features_job.json}"
SOURCE_CATALOG="${2:-src/main/resources/source_catalog}"

# Validate job config file exists
if [ ! -f "$JOB_CONFIG" ]; then
  error_exit "Job configuration file not found: $JOB_CONFIG"
fi

# Validate source catalog directory exists
if [ ! -d "$SOURCE_CATALOG" ]; then
  error_exit "Source catalog directory not found: $SOURCE_CATALOG"
fi

# Print configuration
log "=== Job Configuration ==="
log "Databricks Workspace: $DATABRICKS_SERVER_HOSTNAME"
log "Cluster ID: $DATABRICKS_CLUSTER_ID"
log "Catalog: $DATABRICKS_CATALOG"
log "Schema: $DATABRICKS_SCHEMA"
log "Job Config: $JOB_CONFIG"
log "Source Catalog: $SOURCE_CATALOG"
log "========================="

# Build the project
log "Building the project..."
if ! sbt clean assembly; then
  error_exit "Failed to build the project"
fi

# Find the built JAR file
JAR_FILE=$(find target -name "*.jar" | head -n 1)
if [ -z "$JAR_FILE" ]; then
  error_exit "No JAR file found in target directory after build"
fi
log "Using JAR file: $JAR_FILE"

# Create a temporary directory for config files
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

# Create a temporary application.conf with environment variables
# This file now only contains catalog and schema information, as connection
# details are handled by --remote and direct environment variable access in Scala.
cat > "$TEMP_DIR/application.conf" <<EOL
feature-platform {
  databricks {
    catalog = "${DATABRICKS_CATALOG}"
    schema = "${DATABRICKS_SCHEMA}"
  }
}
EOL

# Submit the job to Databricks
log "Submitting job to Databricks..."
log "Job configuration: $JOB_CONFIG"
log "Source catalog: $SOURCE_CATALOG"

set +e  # Don't exit on error so we can capture the exit code
spark-submit \
  --conf spark.databricks.uc.enabled=true \
  --conf spark.sql.defaultCatalog=temp \
  --conf spark.sql.catalog.temp=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf "spark.sql.warehouse.dir=/databricks/spark/warehouse" \
  --conf "spark.driver.extraJavaOptions=-Dconfig.file=$TEMP_DIR/application.conf" \
  --conf "spark.executor.extraJavaOptions=-Dconfig.file=$TEMP_DIR/application.conf" \
  --conf "spark.sql.catalog.databricks_catalog.catalog.name=${DATABRICKS_CATALOG}" \
  --conf "spark.sql.catalog.databricks_catalog.schema.name=${DATABRICKS_SCHEMA}" \
  --files "$TEMP_DIR/application.conf" \
  --class com.example.featureplatform.runner.JobRunner \
  "$JAR_FILE" \
  "$JOB_CONFIG" \
  "$SOURCE_CATALOG"

# Capture the exit code
SUBMIT_EXIT_CODE=$?

# Clean up
rm -rf "$TEMP_DIR"

# Exit with the spark-submit exit code
exit $SUBMIT_EXIT_CODE
