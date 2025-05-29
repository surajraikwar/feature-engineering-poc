# Running Feature Engineering Jobs on Databricks

This guide explains how to run the feature engineering jobs on a Databricks cluster.

## Prerequisites

1. Install the following on your local machine:
   - Java 8 or 11
   - Scala 2.12
   - sbt 1.5.0 or later
   - Databricks CLI (optional, for cluster management)

2. Set up your Databricks credentials as environment variables:
   ```bash
   export DATABRICKS_SERVER_HOSTNAME=your-workspace-url.cloud.databricks.com
   export DATABRICKS_HTTP_PATH=your-http-path
   export DATABRICKS_TOKEN=your-personal-access-token
   export DATABRICKS_CLUSTER_ID=your-cluster-id
   export DATABRICKS_CATALOG=your-catalog
   export DATABRICKS_SCHEMA=your-schema
   export SPARK_REMOTE=your-spark-remote-connection-string
   ```

## Running the Job

1. **Build the project**
   ```bash
   sbt clean assembly
   ```

2. **Run the job**
   ```bash
   ./run_databricks_job.sh
   ```

   This will:
   - Build the project
   - Submit the job to your Databricks cluster
   - Run the feature engineering job defined in `src/main/resources/configs/jobs/generate_transaction_features_job.yaml`
   - Save the results to the Delta table specified in the job configuration

## Job Configuration

The job configuration is defined in YAML files under `src/main/resources/configs/jobs/`. The main configuration includes:

- Input source configuration
- List of feature transformers
- Output sink configuration

Example job configuration (`generate_transaction_features_job.yaml`):

```yaml
job_name: "generate_transaction_features"
description: "Generates features from transaction data"
input_source:
  name: "temp_fact_mm_transaction_source"
  version: "v1"
feature_transformers:
  - name: "TransactionIndicatorDeriver"
    params: {}
  - name: "TransactionDatetimeDeriver"
    params: {}
  # ... more transformers ...
output_sink:
  sink_type: "delta_table"
  config:
    path: "temp.feature_platform_testing.temp_transaction_features_v1"
    mode: "overwrite"
    options:
      mergeSchema: "true"
```

## Monitoring

You can monitor the job execution in the Databricks workspace:
1. Log in to your Databricks workspace
2. Navigate to the "Jobs" section
3. Look for the running or completed job

## Troubleshooting

- **Connection issues**: Verify your Databricks credentials and cluster status
- **Class not found**: Ensure all dependencies are properly included in the assembly JAR
- **Permission issues**: Check that your Databricks token has the necessary permissions

For more information, refer to the [Databricks documentation](https://docs.databricks.com/).
