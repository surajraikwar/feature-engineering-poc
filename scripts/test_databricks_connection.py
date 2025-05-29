from databricks import sql
import os
from dotenv import load_dotenv

def test_connection():
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    
    print("Testing Databricks connection...")
    print(f"Server: {server_hostname}")
    print(f"HTTP Path: {http_path}")
    
    try:
        # Try to establish a connection
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            print("\n✅ Successfully connected to Databricks!")
            
            # Get catalog and schema from environment or use defaults
            catalog = os.getenv("DATABRICKS_CATALOG", "jupiter")
            schema = os.getenv("DATABRICKS_SCHEMA", "mm")
            
            try:
                # List tables in the specified catalog.schema
                with connection.cursor() as cursor:
                    print(f"\nListing tables in {catalog}.{schema}:")
                    cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
                    tables = cursor.fetchall()
                    
                    if tables:
                        for table in tables:
                            print(f"- {table.tableName}")
                            
                            # Show schema of the first table
                            if table == tables[0]:
                                try:
                                    cursor.execute(f"DESCRIBE TABLE {catalog}.{schema}.{table.tableName}")
                                    print("\nSchema of first table:")
                                    for row in cursor.fetchall():
                                        print(f"  {row.col_name}: {row.data_type}")
                                except Exception as e:
                                    print(f"  Could not describe table: {str(e)}")
                    else:
                        print("No tables found in the specified schema.")
                        
                        # List available schemas in the catalog
                        cursor.execute(f"SHOW SCHEMAS IN {catalog}")
                        schemas = cursor.fetchall()
                        if schemas:
                            print("\nAvailable schemas in the catalog:")
                            for s in schemas:
                                print(f"- {s.databaseName}")
                        
            except Exception as e:
                print(f"\n❌ Error listing tables: {str(e)}")
                
    except Exception as e:
        print(f"\n❌ Failed to connect to Databricks: {str(e)}")
        print("\nPlease check the following:")
        print("1. DATABRICKS_SERVER_HOSTNAME is correctly set")
        print("2. DATABRICKS_HTTP_PATH is correctly set")
        print("3. DATABRICKS_TOKEN is valid and has the necessary permissions")
        print("4. Network connectivity to the Databricks workspace")

if __name__ == "__main__":
    test_connection()
