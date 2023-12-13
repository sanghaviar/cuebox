# Databricks notebook source
# MAGIC %run /Repos/sanghavi.a.r@diggibyte.com/cuebox/bronze_to_silver/orders/orders_utils

# COMMAND ----------

dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("data_lake_base_uri", "", "data_lake_base_uri")
dbutils.widgets.text("database_name", "", "database_name")
dbutils.widgets.text("target_table_name","orders")

database_name = dbutils.widgets.get("database_name")
target_table_name = dbutils.widgets.get("target_table_name")
schedule_date = dbutils.widgets.get("schedule_date")
data_lake_base_uri = dbutils.widgets.get("data_lake_base_uri")

silver_layer_table_name = target_table_name.split(',')[1]
gold_layer_table_name = target_table_name.split(',')[0]

silver_layer_path = data_lake_base_uri+f'/silver/cue-box/{silver_layer_table_name}/'
gold_layer_path = data_lake_base_uri+f'/gold/cue-box/{gold_layer_table_name}/'

# COMMAND ----------

# MAGIC %md
# MAGIC #Extract & Transform

# COMMAND ----------

main_df = read_delta_from_datalake(silver_layer_path).drop('load_date')

# COMMAND ----------

audit_df = main_df.withColumn('at_load_date',lit(schedule_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #Load Data

# COMMAND ----------

write_to_deltalake(audit_df,gold_layer_path,database_name,gold_layer_table_name)
