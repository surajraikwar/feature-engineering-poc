#!/bin/bash

# Set Databricks configuration from environment variables
export DATABRICKS_SERVER_HOSTNAME=dbc-71199ef8-05ae.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/protocolv1/o/555705708876482/0527-081608-f2uhv2zz
export DATABRICKS_TOKEN=dapie22f1a810d0216a01a55f1dae694e6d6
export DATABRICKS_CLUSTER_ID=0527-081608-f2uhv2zz
export DATABRICKS_CATALOG=temp
export DATABRICKS_SCHEMA=feature_platform_testing
export SPARK_REMOTE=sc://dbc-71199ef8-05ae.cloud.databricks.com:443/;token=dapie22f1a810d0216a01a55f1dae694e6d6;x-databricks-cluster-id=0527-081608-f2uhv2zz

# Build the project
sbt clean assembly

# Run the job using spark-submit
spark-submit \
  --class com.example.featureplatform.runner.JobRunner \
  --master $SPARK_REMOTE \
  --deploy-mode client \
  --conf "spark.databricks.service.client.enabled=true" \
  --conf "spark.databricks.service.port=8787" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.databricks.delta.preview.enabled=true" \
  --conf "spark.sql.warehouse.dir=/databricks/spark/warehouse" \
  --conf "spark.sql.catalog.implementation=hive" \
  --conf "spark.sql.legacy.createHiveTableByDefault=false" \
  --conf "spark.databricks.delta.retentionDurationCheck.enabled=false" \
  --conf "spark.databricks.service.address=${DATABRICKS_SERVER_HOSTNAME}:443" \
  --conf "spark.databricks.service.token=${DATABRICKS_TOKEN}" \
  --conf "spark.databricks.service.clusterId=${DATABRICKS_CLUSTER_ID}" \
  --conf "spark.databricks.service.server.enabled=true" \
  --conf "spark.sql.catalog.databricks_catalog.catalog.name=${DATABRICKS_CATALOG}" \
  --conf "spark.sql.catalog.databricks_catalog.schema.name=${DATABRICKS_SCHEMA}" \
  target/scala-2.12/feature-engineering-scala-assembly-0.1.0-SNAPSHOT.jar \
  "src/main/resources/configs/jobs/generate_transaction_features_job.yaml" \
  "src/main/resources/source_catalog"
