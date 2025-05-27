from feature_platform.core.spark import SparkSessionManager
from feature_platform.sources.databricks_spark import DatabricksSparkSource, DatabricksSparkSourceConfig
import os
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    # Initialize Spark session manager
    spark_manager = SparkSessionManager(
        app_name="schema_checker",
        master_url=os.getenv("SPARK_MASTER_URL", "local[*]")
    )
    
    # Set Databricks configuration if available
    if os.getenv("DATABRICKS_SERVER_HOSTNAME"):
        os.environ["DATABRICKS_SERVER_HOSTNAME"] = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        os.environ["DATABRICKS_HTTP_PATH"] = os.getenv("DATABRICKS_HTTP_PATH")
        os.environ["DATABRICKS_TOKEN"] = os.getenv("DATABRICKS_TOKEN")
    
    # Get the Spark session
    with spark_manager as spark:
        # Create a Databricks source config
        from feature_platform.sources.databricks_spark import DatabricksSparkSourceConfig
        
        config = DatabricksSparkSourceConfig(
            name="fact_mm_transaction",
            entity="transaction",
            type="databricks_spark",
            location="fact_mm_transaction",  # Table name only
            format="delta",
            connection_config={
                "server_hostname": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
                "access_token": os.getenv("DATABRICKS_TOKEN"),
                "catalog": "jupiter",
                "schema": "mm"
            },
            options={}
        )
        
        source = DatabricksSparkSource(
            config=config,
            spark_manager=spark_manager
        )
        
        # Read the data using the source
        df = source.read()
        
        # Print schema
        print("Schema of jupiter.mm.fact_mm_transaction:")
        df.printSchema()
        
        # Show sample data
        print("\nSample data (first 5 rows):")
        df.show(5, truncate=False)
        
        # Print row count
        print(f"\nTotal rows: {df.count()}")

if __name__ == "__main__":
    main()
