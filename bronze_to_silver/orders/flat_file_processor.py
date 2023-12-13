# Databricks notebook source
# MAGIC %run ./orders_utils

# COMMAND ----------

dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("data_lake_base_uri", "", "data_lake_base_uri")
dbutils.widgets.text("database_name", "", "database_name")
dbutils.widgets.text("target_table_name","orders")

database_name = dbutils.widgets.get("database_name")
target_table_name = dbutils.widgets.get("target_table_name")
schedule_date = dbutils.widgets.get("schedule_date")
data_lake_base_uri = dbutils.widgets.get("data_lake_base_uri")

bronze_layer_path = data_lake_base_uri+f'/bronze/cue-box/{target_table_name}/{schedule_date}/*'
silver_layer_path = data_lake_base_uri+f'/silver/cue-box/{target_table_name}/'

# COMMAND ----------

main_df = read_csv_from_datalake_bronze_withoutSchema(bronze_layer_path,'true',',','true')

# COMMAND ----------

audit_df = main_df.withColumn('load_date',lit(schedule_date))

# COMMAND ----------

write_to_deltalake(audit_df,silver_layer_path,database_name,target_table_name)
