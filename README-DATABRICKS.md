# Feature Platform - Databricks Deployment (Scala)

This guide explains how to build, package, and deploy the Scala-based feature engineering job to Databricks.

## Prerequisites

1. A Databricks workspace
2. Databricks CLI installed and configured
3. Java 8 or later
4. sbt (Scala Build Tool)
5. Scala 2.12.x

## Project Structure

```
feature-platform/
├── src/
│   └── main/
│       └── scala/
│           └── com/
│               └── featureplatform/
│                   ├── runner/
│                   │   └── FeatureJobRunner.scala
│                   ├── transformers/
│                   │   └── TransactionTransformers.scala
│                   └── tools/
│                       └── DeployToDatabricks.scala
├── configs/
│   └── jobs/
│       └── databricks_transaction_features_job.json
├── databricks-config.json
├── build.sbt
└── README-DATABRICKS.md
```

## Building the Project

1. **Compile the project**:
   ```bash
   sbt compile
   ```

2. **Create a fat JAR**:
   ```bash
   sbt assembly
   ```
   This will create a JAR file in `target/scala-2.12/` that includes all dependencies.

## Deployment

### 1. Using the Scala Deployment Tool

We provide a Scala-based deployment tool to simplify the deployment process:

```bash
# Compile the deployment tool
sbt "runMain com.featureplatform.tools.DeployToDatabricks"

# Available commands:
#   setup       - Set up the required DBFS directory structure
#   deploy      - Deploy code and configurations to DBFS
#   create-job  - Create a new Databricks job
#   run-job <id> - Run an existing Databricks job
#   all         - Run all deployment steps

# Example: Run all deployment steps
sbt "runMain com.featureplatform.tools.DeployToDatabricks all"
```

### 2. Manual Deployment

Alternatively, you can deploy manually:

1. **Upload code to DBFS**:
   ```bash
   # Create directory structure
   databricks fs mkdirs dbfs:/FileStore/feature-platform/
   
   # Upload the assembly JAR
   databricks fs cp --overwrite target/scala-2.12/feature-platform-assembly-1.0.jar dbfs:/FileStore/feature-platform/
   
   # Upload configurations
   databricks fs cp --overwrite --recursive configs/ dbfs:/FileStore/feature-platform/configs/
   ```

2. **Create a Databricks Job**:
   ```bash
   databricks jobs create --json @databricks-config.json
   ```

3. **Run the Job**:
   ```bash
   databricks jobs run-now --job-id <job-id>
   ```

## Configuration

### Job Configuration (`configs/jobs/databricks_transaction_features_job.json`)

This file defines the feature engineering pipeline:
- Input source configuration
- List of feature transformers
- Output sink settings
- Execution parameters

### Cluster Configuration (`databricks-config.json`)

This file defines the Databricks job configuration:
- Cluster specifications
- Spark configuration
- Autoscaling settings
- Library dependencies

## Monitoring and Debugging

1. **Logs**: View job logs in the Databricks UI under Jobs > [Job Name] > Runs
2. **Spark UI**: Access the Spark UI through the Databricks cluster UI
3. **Metrics**: Monitor job performance and resource usage in the cluster metrics

## Best Practices

1. **Version Control**:
   - Keep all configuration files in version control
   - Use tags or branches for different environments

2. **Testing**:
   - Test with small datasets before full deployment
   - Implement unit tests for transformers

3. **Performance**:
   - Monitor and tune Spark configurations
   - Use Delta Lake for reliable data processing
   - Implement proper partitioning for large datasets

4. **Error Handling**:
   - Implement comprehensive error handling in transformers
   - Set up alerts for job failures
   - Use Delta Lake's ACID transactions for data consistency

5. **Documentation**:
   - Document all feature transformations
   - Keep configuration documentation up to date
   - Maintain a changelog for the feature platform
