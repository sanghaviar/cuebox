# Databricks notebook source
# MAGIC %md
# MAGIC # IMPORT

# COMMAND ----------

# MAGIC %run /Repos/sanghavi.a.r@diggibyte.com/cuebox/bronze_to_silver/orders/orders_utils

# COMMAND ----------

dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("data_lake_base_uri", "", "data_lake_base_uri")
dbutils.widgets.text("database_names", "", "database_names")
dbutils.widgets.text("table_names","","table_names")
dbutils.widgets.text("target_path","","target_path")

data_lake_base_uri = dbutils.widgets.get("data_lake_base_uri")
database_names = dbutils.widgets.get("database_names").split(",")
table_names = dbutils.widgets.get("table_names").split(",")
target_path = dbutils.widgets.get("target_path")
schedule_date = dbutils.widgets.get("schedule_date")

# COMMAND ----------

path = f"{data_lake_base_uri}/gold/{target_path}/{table_names[1]}"

# COMMAND ----------

schedule_date = datetime.strptime(schedule_date.split('T')[0],'%Y-%m-%d')

# COMMAND ----------

SELECT_FIELDS = ["order_number","event_date","order_status_code","total_price","total_discounts","total_tax","order_placed_date","bundle_ids","payment_status","fs_channel_code","fs_channel_sales_channel_type_code","currency_iso_code","site_uid","entries_quantity","entries_total_price","entries_a_service","entries_total_tax","entries_discount_rows","delivery_address_email","delivery_status_code","entries_product_code","entries_parent_entry_entry_number","entries_promotion_code","cart_level_promotions","country_code","entries_discount_name","entries_entry_number","fs_channel_name","load_date"]

MERGE_COLUMNS = ["order_number", "event_date","entries_entry_number","entries_product_code"]

PARTITION_COLUMNS = ["country_code"]

# COMMAND ----------

SELECTED_FIELDS = ["order_number","event_date","order_status_code","total_price","total_discounts","total_tax","order_placed_date","bundle_ids","payment_status","fs_channel_code","fs_channel_sales_channel_type_code","currency_iso_code","site_uid","entries_quantity","entries_total_price","entries_a_service","entries_total_tax","entries_discount_rows","customer_id","delivery_status_code","entries_product_code","entries_parent_entry_entry_number","entries_promotion_code","cart_level_promotions","country_code","entries_discount_name","entries_entry_number","fs_channel_name","load_date","at_load_date","mp_flag","net_sales_order","net_sales_entries"]

# COMMAND ----------

# MAGIC %md
# MAGIC # EXTRACT

# COMMAND ----------

orders_data_from_silver = spark.sql("SELECT * FROM {}.{}".format(database_names[0], table_names[0])).select(*SELECT_FIELDS).\
                                withColumn("load_date", to_timestamp(col('load_date')))

# COMMAND ----------

# MAGIC %md
# MAGIC # TRANSFORM

# COMMAND ----------

windows_spec = Window.partitionBy("order_number").orderBy(col("event_date").desc())

# COMMAND ----------

rank_orders_on_event_date = orders_data_from_silver.withColumn("rank", rank().over(windows_spec))

get_latest_order_event = rank_orders_on_event_date.filter("rank == '1'").drop("rank").dropDuplicates()

add_load_date = get_latest_order_event.withColumn("at_load_date", lit(schedule_date))

mp_flag_df=add_load_date.withColumn("mp_flag",when(col("fs_channel_code").rlike("(MP)"),"True").otherwise("False"))

# COMMAND ----------

currency_rate_df = spark.sql("select * from silver.currency_exchange").drop('load_date')

# COMMAND ----------

joined_df = mp_flag_df.join(currency_rate_df,mp_flag_df.currency_iso_code == currency_rate_df.ref_currency,"left")

net_sales_order_df = joined_df.withColumn("net_sales_order",(col("total_price")- col("total_tax"))*col("rate"))

net_sales_entries_df = net_sales_order_df.withColumn("net_sales_entries",when(col("entries_total_tax").isNull(),col("entries_total_price")*col("rate")).otherwise((col("entries_total_price")- col("entries_total_tax"))*col("rate"))).withColumnRenamed('delivery_address_email','email')

# COMMAND ----------

customers = spark.table('gold.dim_consumers').select('emai_hash','customer_id').toDF('email','customer_id')

# COMMAND ----------

selected_df = net_sales_entries_df.join(customers,['email'],'left').select(*SELECTED_FIELDS)

# COMMAND ----------

final_df = selected_df.withColumn("country_code",substring(col("country_code"),1,2))\
                      .withColumn("event_date", to_timestamp(col('event_date')))\
                      .withColumn("order_placed_date", to_timestamp(col('order_placed_date')))

# COMMAND ----------

# MAGIC %md
# MAGIC # LOAD

# COMMAND ----------

delta_overwrite(final_df, database_names[1], table_names[1], MERGE_COLUMNS, path, PARTITION_COLUMNS)
