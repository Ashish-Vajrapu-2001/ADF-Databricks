# Databricks notebook source
# DBTITLE 1,Initialize Watermark Management
# Parameter definitions (passed from ADF)
dbutils.widgets.text("action", "get", "Action (get/set)")
dbutils.widgets.text("source_system", "default", "Source System")
dbutils.widgets.text("source_schema", "default", "Source Schema")
dbutils.widgets.text("source_entity", "default", "Source Entity")
dbutils.widgets.text("new_watermark", "1900-01-01 00:00:00", "New Watermark (for set action)")
dbutils.widgets.text("environment", "dev", "Environment (dev/test/prod)") # Added for UC catalog naming

# COMMAND ----------

# DBTITLE 1,Retrieve Parameter Values
action = dbutils.widgets.get("action")
source_system = dbutils.widgets.get("source_system")
source_schema = dbutils.widgets.get("source_schema")
source_entity = dbutils.widgets.get("source_entity")
new_watermark = dbutils.widgets.get("new_watermark")
environment = dbutils.widgets.get("environment")

# Construct the full table identifier for watermarks in Unity Catalog
# Assuming a dedicated control catalog for metadata, or using the bronze catalog
catalog_name = f"{environment}_control" # Or use f"{environment}_bronze"
schema_name = "metadata"
watermark_table_name = f"{catalog_name}.{schema_name}.watermarks"

# COMMAND ----------

# DBTITLE 1,Ensure Watermark Table Exists
# Create the metadata schema and watermark table if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {watermark_table_name} (
    source_system STRING,
    source_schema STRING,
    source_entity STRING,
    last_watermark TIMESTAMP,
    updated_at TIMESTAMP
  ) USING DELTA
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');
""")

# COMMAND ----------

# DBTITLE 1,Perform Watermark Action
if action == "get":
    # Get last watermark
    df_watermark = spark.sql(f"""
        SELECT last_watermark FROM {watermark_table_name}
        WHERE source_system = '{source_system}'
        AND source_schema = '{source_schema}'
        AND source_entity = '{source_entity}'
    """)

    if df_watermark.count() > 0:
        last_watermark = df_watermark.collect()[0]["last_watermark"].strftime('%Y-%m-%d %H:%M:%S')
        print(f"Retrieved watermark: {last_watermark}")
    else:
        # Default/initial watermark
        last_watermark = "1900-01-01 00:00:00"
        print(f"No watermark found, using default: {last_watermark}")

    # Pass the watermark back to ADF
    dbutils.notebook.exit({"last_watermark": last_watermark})

elif action == "set":
    # Set new watermark
    spark.sql(f"""
        MERGE INTO {watermark_table_name} AS target
        USING (SELECT '{source_system}' AS source_system,
                      '{source_schema}' AS source_schema,
                      '{source_entity}' AS source_entity,
                      '{new_watermark}' AS new_watermark_val,
                      now() AS updated_ts) AS source
        ON target.source_system = source.source_system
           AND target.source_schema = source.source_schema
           AND target.source_entity = source.source_entity
        WHEN MATCHED THEN
            UPDATE SET target.last_watermark = to_timestamp(source.new_watermark_val, 'yyyy-MM-dd HH:mm:ss'),
                       target.updated_at = source.updated_ts
        WHEN NOT MATCHED THEN
            INSERT (source_system, source_schema, source_entity, last_watermark, updated_at)
            VALUES (source.source_system, source.source_schema, source.source_entity, to_timestamp(source.new_watermark_val, 'yyyy-MM-dd HH:mm:ss'), source.updated_ts);
    """)
    print(f"Set watermark for {source_system}.{source_schema}.{source_entity} to {new_watermark}")
    dbutils.notebook.exit({"status": "Success", "new_watermark_set": new_watermark})

else:
    raise ValueError(f"Invalid action: {action}. Must be 'get' or 'set'.")
