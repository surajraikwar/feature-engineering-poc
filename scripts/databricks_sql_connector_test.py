from databricks import sql
import os
from dotenv import load_dotenv

def main():
    # Load environment variables
    load_dotenv()
    
    # Get connection parameters
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")
    
    if not all([server_hostname, http_path, access_token]):
        print("Error: Missing required Databricks connection parameters in environment variables.")
        print("Please set DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN")
        return
    
    print(f"Connecting to Databricks workspace: {server_hostname}")
    
    try:
        # Connect to Databricks SQL endpoint
        with sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                # List all catalogs
                print("\nAvailable catalogs:")
                cursor.execute("SHOW CATALOGS")
                catalogs = [row.catalog for row in cursor.fetchall()]
                for catalog in catalogs:
                    print(f"- {catalog}")
                
                # For each catalog, list schemas and tables
                for catalog in catalogs:
                    try:
                        print(f"\nCatalog: {catalog}")
                        cursor.execute(f"SHOW SCHEMAS IN {catalog}")
                        schemas = [row.databaseName for row in cursor.fetchall()]
                        
                        for schema in schemas:
                            try:
                                print(f"  Schema: {schema}")
                                cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
                                tables = cursor.fetchall()
                                
                                if tables:
                                    for table in tables:
                                        print(f"    - {table.tableName}")
                                        
                                        # Show first row of the table
                                        try:
                                            cursor.execute(f"SELECT * FROM {catalog}.{schema}.{table.tableName} LIMIT 1")
                                            columns = [desc[0] for desc in cursor.description]
                                            row = cursor.fetchone()
                                            if row:
                                                print("      Columns:", ", ".join(columns))
                                                print("      First row:", row)
                                        except Exception as e:
                                            print(f"      Could not read table: {str(e)}")
                                else:
                                    print("    No tables found")
                                    
                            except Exception as e:
                                print(f"    Error listing tables in {catalog}.{schema}: {str(e)}")
                    except Exception as e:
                        print(f"  Error listing schemas in {catalog}: {str(e)}")
                        
    except Exception as e:
        print(f"Error connecting to Databricks: {str(e)}")

if __name__ == "__main__":
    main()
