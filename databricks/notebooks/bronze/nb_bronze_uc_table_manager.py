# Databricks notebook source
# DBTITLE 1,Initialize UC Table Manager
# Parameter definitions (passed from ADF)
dbutils.widgets.text("source_system", "default", "Source System")
dbutils.widgets.text("source_schema", "default", "Source Schema (source's schema)")
dbutils.widgets.text("source_entity", "default", "Source Entity (source's table name)")
dbutils.widgets.text("target_format", "parquet", "Target file format (parquet/json)")
dbutils.widgets.text("container_name", "bronze", "ADLS Gen2 container name")
dbutils.widgets.text("adls_gen2_name", "<your_adls_gen2_name>", "ADLS Gen2 storage account name")
dbutils.widgets.text("environment", "dev", "Environment (dev/test/prod)") # Added for UC catalog naming

# COMMAND ----------

# DBTITLE 1,Retrieve Parameter Values
source_system = dbutils.widgets.get("source_system")
source_schema = dbutils.widgets.get("source_schema")
source_entity = dbutils.widgets.get("source_entity")
target_format = dbutils.widgets.get("target_format")
container_name = dbutils.widgets.get("container_name")
adls_gen2_name = dbutils.widgets.get("adls_gen2_name")
environment = dbutils.widgets.get("environment")

# Construct Unity Catalog identifiers
uc_catalog = f"{environment}_bronze"
uc_schema = source_system.lower() # Use source system as UC schema name
uc_table_name = f"{uc_catalog}.{uc_schema}.{source_entity.lower()}"

# Construct ADLS Gen2 path pattern for the Bronze data
# The path is dynamic due to partitioning by year/month/day
adls_path_base = f"abfss://{container_name}@{adls_gen2_name}.dfs.core.windows.net/bronze/{source_system}/{source_schema}/{source_entity}"

# COMMAND ----------

# DBTITLE 1,Create/Alter External Delta Table in Unity Catalog
# Ensure the catalog and schema exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {uc_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {uc_catalog}.{uc_schema}")

# For Bronze, we register external tables over the raw files.
# The `MERGE SCHEMA` option is crucial for handling schema evolution in the Bronze layer.
# We infer schema from the latest data landed by ADF.

try:
    # Check if table already exists
    table_exists = spark.sql(f"SHOW TABLES IN {uc_catalog}.{uc_schema} LIKE '{source_entity.lower()}'").count() > 0

    if not table_exists:
        print(f"Creating new external table: {uc_table_name}")
        spark.sql(f"""
            CREATE EXTERNAL TABLE {uc_table_name}
            USING {target_format}
            OPTIONS (
              'path' = '{adls_path_base}'
            );
        """)
    else:
        print(f"Table {uc_table_name} already exists. Attempting to repair and evolve schema if needed.")
        # For Delta tables, simply running OPTIMIZE or inserting new data can handle schema evolution.
        # For external tables over Parquet/JSON, we can refresh and infer schema.
        # This approach re-creates if needed, but a more robust way might be ALTER TABLE ADD COLUMNS
        # or using DLT for auto-schema evolution. For raw bronze, re-inferring/refreshing is common.

        # Refresh table metadata to pick up new files/partitions
        spark.sql(f"REFRESH TABLE {uc_table_name}")

        # If schema evolution is expected in raw files, you might need to drop and re-create.
        # For production, consider using DLT's `APPLY CHANGES INTO` for robust schema evolution
        # or defining a strict schema and letting DLT handle invalid rows.
        # For a truly raw Bronze layer, allowing schema inference is often desired.

        # To ensure the schema is up-to-date with newly landed files,
        # we can infer schema from the base path and potentially alter the table.
        # For simplicity and robust initial setup, let's keep it creating/refreshing.
        # If the schema truly changes drastically, a drop-and-create might be necessary,
        # but for new columns, `REFRESH TABLE` often suffices for Delta, and for external non-Delta,
        # new reads will pick up new columns.
        print(f"Table {uc_table_name} refreshed.")

    print(f"Successfully managed external table {uc_table_name} for path {adls_path_base}")
    dbutils.notebook.exit({"status": "Success", "table_name": uc_table_name, "path": adls_path_base})

except Exception as e:
    error_message = f"Failed to manage external table {uc_table_name}: {str(e)}"
    print(error_message)
    raise Exception(error_message) # Raise exception to fail ADF activity
