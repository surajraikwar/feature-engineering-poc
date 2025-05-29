#!/bin/bash
# Script to upload feature platform code to DBFS and submit a job to Databricks

# Source environment variables
source .env 2>/dev/null || echo "Warning: .env file not found. Make sure environment variables are set."

# Check if environment variables are set
if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ] || [ -z "$DATABRICKS_CLUSTER_ID" ]; then
    echo "Error: Required environment variables are missing. Please set DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_CLUSTER_ID."
    exit 1
fi

# Activate virtual environment if it exists
if [ -d ".venv-3.10" ]; then
    source .venv/bin/activate
    echo "Activated Python virtual environment."
fi

# Create directories on DBFS
echo "Submitting feature generation job to Databricks..."

# Run the job submission script
python3 scripts/submit_to_databricks.py \
    --job-name "Generate Transaction Features" \
    --job-config "configs/jobs/generate_temp_transaction_features_job.yaml" \
    --source-catalog "source"

# Check if job submission was successful
if [ $? -eq 0 ]; then
    echo "Job submitted successfully. Check the Databricks UI for job status."
else
    echo "Failed to submit job. Check the logs for errors."
    exit 1
fi
