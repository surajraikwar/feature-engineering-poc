from pyspark.sql import SparkSession
from domain.core.spark import SparkSessionManager
import os
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    # Initialize Spark session manager
    spark_manager = SparkSessionManager(
        app_name="databricks_tables_lister",
        master_url=os.getenv("SPARK_MASTER_URL", "local[*]")
    )
    
    # Get the Spark session
    with spark_manager as spark:
        # Set Databricks connection properties
        spark.conf.set("spark.databricks.service.server.enabled", "true")
        spark.conf.set("spark.databricks.service.port", "15001")
        
        # Get catalog and schema from environment or use defaults
        catalog = os.getenv("DATABRICKS_CATALOG", "jupiter")
        schema = os.getenv("DATABRICKS_SCHEMA", "mm")
        
        print(f"Listing tables in {catalog}.{schema}:")
        
        try:
            # List all tables in the specified catalog and schema
            tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
            tables_df.show(truncate=False)
            
            # If tables exist, show the first row of each table
            if tables_df.count() > 0:
                print("\nSample data from each table:")
                for row in tables_df.collect():
                    table_name = row['tableName']
                    full_table_name = f"{catalog}.{schema}.{table_name}"
                    try:
                        print(f"\nTable: {full_table_name}")
                        spark.table(full_table_name).show(1, truncate=False)
                    except Exception as e:
                        print(f"  Could not read table {full_table_name}: {str(e)}")
                        
        except Exception as e:
            print(f"Error listing tables: {str(e)}")
            print("Available catalogs:")
            try:
                spark.sql("SHOW CATALOGS").show(truncate=False)
            except Exception as e2:
                print(f"Could not list catalogs: {str(e2)}")

if __name__ == "__main__":
    main()
