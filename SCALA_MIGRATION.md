# Scala Migration for Feature Platform

This document outlines the migration of the feature platform job execution from Python to Scala.

## Project Structure

```
feature-platform/
├── src/
│   └── main/
│       └── scala/
│           └── com/
│               └── featureplatform/
│                   └── runner/
│                       └── FeatureJobRunner.scala  # Main job runner
├── build.sbt                                      # SBT build configuration
└── submit_scala_job.py                            # Script to build and submit jobs
```

## Prerequisites

1. Java 8 or 11
2. Scala 2.12.x
3. sbt (Scala Build Tool)
4. Python 3.7+ (for the submission script)
5. Databricks CLI configured with access token

## Setup

1. **Install sbt**:
   ```bash
   # On macOS
   brew install sbt
   
   # On Ubuntu/Debian
   echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
   echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_oldlist.list
   curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
   sudo apt-get update
   sudo apt-get install sbt
   ```

2. **Install Python dependencies**:
   ```bash
   pip install requests
   ```

## Building the Project

To build the Scala project and create a fat JAR:

```bash
sbt assembly
```

The JAR will be created at `target/scala-2.12/feature-platform.jar`.

## Running Jobs

To submit a job to Databricks:

```bash
# Set up Databricks configuration
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-personal-access-token"

# Build the project
sbt assembly

# Submit the job using Databricks CLI
databricks jobs submit \
  --json '{
    "name": "Transaction Features Job",
    "existing_cluster_id": "your-cluster-id",
    "libraries": [
      {
        "jar": "dbfs:/FileStore/feature-platform/jars/feature-platform.jar"
      }
    ],
    "spark_jar_task": {
      "main_class_name": "com.featureplatform.runner.FeatureJobRunner",
      "parameters": [
        "dbfs:/FileStore/feature-platform/jobs/generate_transaction_features_job.yaml"
      ]
    },
    "max_retries": 1,
    "timeout_seconds": 3600
  }'
```

Or using the Python script after uploading the JAR and config:

```bash
# First upload the JAR and config
databricks fs cp --overwrite target/scala-2.12/feature-platform.jar dbfs:/FileStore/feature-platform/jars/
databricks fs cp --overwrite configs/jobs/generate_transaction_features_job.yaml dbfs:/FileStore/feature-platform/jobs/

# Then run the Python script
python submit_scala_job.py \
  --job-config configs/jobs/generate_transaction_features_job.yaml \
  --job-name "Transaction Features Job"
```

### Arguments

- `--job-config`: Path to the job configuration YAML file
- `--job-name`: Name of the job (will be displayed in Databricks UI)
- `--skip-build`: Skip building the project (use if you've already built the JAR)
- `--debug`: Enable debug logging

## Job Configuration

Job configurations are defined in YAML files in the `configs/jobs/` directory. Example:

```yaml
job_name: "transaction_features"
input_source:
  name: "temp_fact_mm_transaction_source"
  version: "v1"
feature_transformers:
  - name: "TransactionStatusDeriver"
    params:
      status_field: "status"
  - name: "TransactionChannelDeriver"
    params:
      channel_field: "channel"
output_sink:
  sink_type: "delta"
  config:
    path: "dbfs:/user/hive/warehouse/transaction_features"
    mode: "overwrite"
```

## Adding New Transformers

To add a new feature transformer:

1. Add a new case in the `applyTransformer` method in `FeatureJobRunner.scala`
2. Implement the transformation logic
3. Update your job configuration to include the new transformer

## Monitoring Jobs

Jobs can be monitored through the Databricks workspace UI. The submission script will output a URL to track the job run.

## Troubleshooting

- **Build issues**: Make sure you have the correct Java and Scala versions installed
- **Permission issues**: Ensure your Databricks token has the necessary permissions
- **Class not found**: Verify the main class name in `submit_scala_job.py` matches your package and class name

## Performance Considerations

- The Scala implementation should provide better performance for large datasets
- Consider enabling dynamic allocation and tuning Spark parameters for your workload
- Monitor resource usage in the Databricks UI to identify bottlenecks
