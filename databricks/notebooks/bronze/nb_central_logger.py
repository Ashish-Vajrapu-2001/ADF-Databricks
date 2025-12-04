# Databricks notebook source
# DBTITLE 1,Initialize Central Logger
# Parameter definitions (passed from ADF)
dbutils.widgets.text("pipeline_name", "unknown_pipeline", "ADF Pipeline Name")
dbutils.widgets.text("run_id", "unknown_run_id", "ADF Pipeline Run ID")
dbutils.widgets.text("status", "Info", "Status (Succeeded/Failed/Info)")
dbutils.widgets.text("message", "No message", "Log Message")
dbutils.widgets.text("source_system", "N/A", "Source System (if applicable)")
dbutils.widgets.text("source_entity", "N/A", "Source Entity (if applicable)")
dbutils.widgets.text("rows_copied", "0", "Rows Copied (if applicable)")
dbutils.widgets.text("data_read", "0", "Data Read (bytes, if applicable)")
dbutils.widgets.text("data_written", "0", "Data Written (bytes, if applicable)")
dbutils.widgets.text("environment", "dev", "Environment (dev/test/prod)") # Added for UC catalog naming

# COMMAND ----------

# DBTITLE 1,Retrieve Parameter Values
pipeline_name = dbutils.widgets.get("pipeline_name")
run_id = dbutils.widgets.get("run_id")
status = dbutils.widgets.get("status")
message = dbutils.widgets.get("message")
source_system = dbutils.widgets.get("source_system")
source_entity = dbutils.widgets.get("source_entity")
rows_copied = int(dbutils.widgets.get("rows_copied"))
data_read = int(dbutils.widgets.get("data_read"))
data_written = int(dbutils.widgets.get("data_written"))
environment = dbutils.widgets.get("environment")

# Construct Unity Catalog table name for logging
catalog_name = f"{environment}_control" # Or f"{environment}_bronze"
schema_name = "logs"
log_table_name = f"{catalog_name}.{schema_name}.adf_pipeline_logs"

# COMMAND ----------

# DBTITLE 1,Ensure Log Table Exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {log_table_name} (
    log_timestamp TIMESTAMP,
    pipeline_name STRING,
    run_id STRING,
    status STRING,
    message STRING,
    source_system STRING,
    source_entity STRING,
    rows_copied BIGINT,
    data_read_bytes BIGINT,
    data_written_bytes BIGINT,
    environment STRING
  ) USING DELTA
  TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true');
""")

# COMMAND ----------

# DBTITLE 1,Log Pipeline Details
try:
    log_data = [(
        spark.sql("SELECT current_timestamp()").collect()[0][0],
        pipeline_name,
        run_id,
        status,
        message,
        source_system,
        source_entity,
        rows_copied,
        data_read,
        data_written,
        environment
    )]

    df_log = spark.createDataFrame(log_data, schema=[
        "log_timestamp", "pipeline_name", "run_id", "status", "message",
        "source_system", "source_entity", "rows_copied", "data_read_bytes", "data_written_bytes", "environment"
    ])

    df_log.write.format("delta").mode("append").saveAsTable(log_table_name)
    print(f"Logged ADF pipeline event: {pipeline_name} - {status} - {message}")
    dbutils.notebook.exit({"status": "Success", "log_entry_added": True})

except Exception as e:
    error_message = f"Failed to log pipeline event to {log_table_name}: {str(e)}"
    print(error_message)
    # Re-raise the exception to indicate failure in ADF
    raise Exception(error_message)
