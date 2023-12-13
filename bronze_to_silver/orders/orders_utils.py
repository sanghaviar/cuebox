# Databricks notebook source
# MAGIC %md 
# MAGIC #Imports

# COMMAND ----------

# DBTITLE 1,Required Import(s)
from datetime import datetime, timedelta
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import *
from pyspark.sql.window import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Functions/UDFs

# COMMAND ----------

# DBTITLE 1,Function to Convert Unix Timestamp to Normal timestamp
# Define a UDF to extract the timestamp from the string
def extract_timestamp(s):
    return int(s.replace("/Date(", "").replace(")/", ""))
  
def epoch_to_timestamp(epoch_time):
  if epoch_time is None:
    return ""
  else:
    epoch_time_int = int(extract_timestamp(epoch_time))/1000
    return datetime.fromtimestamp(epoch_time_int).strftime('%Y-%m-%d %H:%M:%S')
  
epoch_to_timestamp_udf = udf(lambda z: epoch_to_timestamp(z),StringType())

# COMMAND ----------

# DBTITLE 1,Function to Flatten the JSON
def flatten_json_df(df):
    """
    Takes a PySpark dataframe with nested JSON columns and returns a flattened dataframe.
    """
     
    # Get a list of all the columns in the dataframe
    collect_types = [df.select(cols).schema[0].dataType for cols in df.columns]
    columns = df.columns

    # Loop through each column and check if it contains a nested JSON structure
    for column in columns:
        # Check if the column contains a struct or array type
        column_type = df.select(column).schema[0].dataType
        if isinstance(column_type, StructType):
            # If the column is a struct type, create new columns for each field
            struct_fields = [col(column + '.' + field.name).alias(column + '_' + field.name)
                             for field in column_type]
            df = df.select("*", *struct_fields).drop(column)
            
        elif isinstance(column_type, ArrayType) and isinstance(column_type.elementType, StructType):
            # If the column is an array type of struct, explode the column and create new columns for each field
            df = df.withColumn(column, explode_outer(column))
            struct_fields = [col(column + '.' + field.name).alias(column + '_' + field.name)
                             for field in column_type.elementType]
            df = df.select("*", *struct_fields).drop(column)

        elif isinstance(column_type, ArrayType) and isinstance(column_type.elementType, ArrayType):
            # If the column is an array type of array type, explode the column and call the function recursively
            df = df.withColumn(column, explode_outer(column))
            nested_type = column_type.elementType
            if isinstance(nested_type, StructType):
                # If the nested type is a struct type, create new columns for each field
                struct_fields = [col(column + '.' + field.name).alias(column + '_' + field.name)
                                 for field in nested_type]
                df = df.select("*", *struct_fields).drop(column)
            elif isinstance(nested_type, ArrayType):
                # If the nested type is an array type, call the function recursively
                nested_elem_type = nested_type.elementType
                if isinstance(nested_elem_type, StructType):
                    df = flatten_json_df(df)
    
    flattned_cols = list()
    non_flattned_cols = list()
    for cols in df.columns:
      col_type = df.select(cols).schema[0].dataType
      if isinstance(col_type, StringType) or isinstance(col_type, DoubleType) or isinstance(col_type, LongType) or isinstance(col_type, BooleanType):
        flattned_cols.append(cols)
      else:
        non_flattned_cols.append(cols)
      
    if len(non_flattned_cols) > 0:
      return flatten_json_df(df)
    
    return df


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Read Functions

# COMMAND ----------

# DBTITLE 1,Read csv from bronze
# To be deprecated
# Method to read the file as dataframe from datalake
#  @param  path   : bronze layer location
#  @param  header   : header of the file as true or false
#  @param  delimiter   : mentioned in the file
#  @param  inferSchema   : initialize schema to the file
#  @return dataframe

def read_csv_from_datalake_bronze_withoutSchema(path, header, delimiter, inferSchema):

  df = spark.read.option("inferSchema",inferSchema)\
            .option("header", header)\
            .option("delimiter", delimiter)\
            .csv(path)
  return df

# COMMAND ----------

# DBTITLE 1,Read csv from bronze with Custom Schema
# To be deprecated
# Method to read the file as dataframe from datalake
#  @param  path   : bronze layer location
#  @param  header   : header of the file as true or false
#  @param  delimiter   : mentioned in the file
#  @param  schema   : custom schema
#  @return dataframe

def read_csv_from_datalake_bronze(path, header, delimiter, schema, badRecordPath):

  df = spark.read.schema(schema)\
            .option("multiline","true")\
            .option("badRecordsPath",badRecordPath)\
            .option("header", header)\
            .option("delimiter", delimiter)\
            .csv(path)
  return df

# COMMAND ----------

# DBTITLE 1,Read JSON File from - Data Lake
# Method to read the file as dataframe from datalake
#  @param  path   : bronze layer location
#  @param  option_dict : all the needed options in form of a dictonary
#  @param  schema   : custom schema
def read_json_from_datalake(path, options_dict):
  return spark.read.options(**options_dict).json(path)

# COMMAND ----------

# DBTITLE 1,Read CSV File From - Data Lake
# Method to read the file as dataframe from datalake
#  @param  path   : bronze layer location
#  @param  option_dict : all the needed options in form of a dictonary
#  @param  schema   : custom schema
def read_csv_from_datalake_with_options(path, options_dict, schema):
  return spark.read.options(**options_dict).schema(schema).csv(path)

# COMMAND ----------

# DBTITLE 1,Read Delta File
def read_delta_from_datalake(path):
  return spark.read.format("delta").load(path)

# COMMAND ----------

# DBTITLE 1,Read Parquet File
def read_parquet_from_datalake(path):
  return spark.read.parquet(path)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Custom Schema

# COMMAND ----------

# DBTITLE 1,Structure(s) for Schema - Order
common_code = StructType([
StructField("code", StringType(), True)])

status = StructType([
StructField("code", StringType(), True)])

consignments = StructType([
StructField("shippingDate", StringType(), True),\
StructField("trackingID", StringType(), True),\
StructField("code", StringType(), True),\
StructField("deliveredDate", StringType(), True),\
StructField("namedDeliveryDate", StringType(), True),\
StructField("statusDisplay", StringType(), True)])


promotion = StructType([
StructField("code", StringType(), True)])


promotion_results = StructType([
StructField("consumedEntries", StringType(), True),\
StructField("promotion", promotion, True)])

order_block = StructType([
StructField("code", StringType(), True),\
StructField("codeLowerCase", StringType(), True)])


delivery_address = StructType([
StructField("isBusinessUser", BooleanType(), True),\
StructField("town", StringType(), True),\
StructField("email", StringType(), True),\
StructField("postalcode", StringType(), True)])


fs_channel = StructType([
StructField("name", StringType(), True),\
StructField("code", StringType(), True),\
StructField("salesChannelType", common_code, True)])


currency = StructType([
StructField("isoCode", StringType(), True)])


site = StructType([
StructField("uid", StringType(), True)])


product = StructType([
StructField("code", StringType(), True)])


parent_entry = StructType([
StructField("entryNumber", LongType(), True)
])

payment_info = StructType([
StructField("code", StringType(), True),\
StructField("adyenPaymentMethodName", StringType(), True),\
StructField("adyenPaymentMethod", StringType(), True)])


user = StructType([
StructField("uid", StringType(), True),\
StructField("referredByCode", StringType(), True),\
StructField("referredByStaffName", StringType(), True),\
StructField("type", common_code, True)])


entry = StructType([
StructField("product", product, True),\
StructField("quantity", StringType(), True),\
StructField("totalPrice", DoubleType(), True),\
StructField("isFocusedPrice", BooleanType(), True),\
StructField("parentEntry", parent_entry, True),\
StructField("aService", BooleanType(), True),\
StructField("entryNumber", LongType(), True),\
StructField("d2cPrice", DoubleType(), True),\
StructField("productInfo", StringType(), True),\
StructField("basePrice", DoubleType(), True),\
StructField("order", common_code, True),\
StructField("discountName", StringType(), True),\
StructField("percentageTax", StringType(), True),\
StructField("totalTax", StringType(), True),\
StructField("discountRows", DoubleType(), True)])

# COMMAND ----------

# DBTITLE 1,Custom Schema - Order
order_custom_schema = StructType([
StructField("code", StringType(), True),\
StructField("sapRejectionReason", StringType(), True),\
StructField("trackingUrl", StringType(), True),\
StructField("salesOrderNumber", StringType(), True),\
StructField("sapGoodsIssueDate", StringType(), True),\
StructField("versionID", StringType(), True),\
StructField("sapPlantCode", StringType(), True),\
StructField("trackingId", StringType(), True),\
StructField("orderCreatedDateInECC", StringType(), True),\
StructField("creationTime", StringType(), True),\
StructField("totalPrice", DoubleType(), True),\
StructField("totalDiscounts", DoubleType(), True),\
StructField("allPromotionResults", ArrayType(promotion_results), True),\
StructField("totalTax", DoubleType(), True),\
StructField("orderPlacedDate", StringType(), True),\
StructField("deliveryAddress", delivery_address, True),\
StructField("deliveryNumber", StringType(), True),\
StructField("productRegistration", BooleanType(), True),\
StructField("csCode", StringType(), True),\
StructField("bundleIds", StringType(), True),\
StructField("consignments", ArrayType(consignments), True),\
StructField("orderblock", order_block, True),\
StructField("fsChannel", fs_channel, True),\
StructField("currency", currency, True),\
StructField("servicesSubTotalWithoutDiscounts", DoubleType(), True),\
StructField("paymentStatus", StringType(), True),\
StructField("appliedCoupons", StringType(), True),\
StructField("servicesSubTotal", DoubleType(), True),\
StructField("utmDetails", StringType(), True),\
StructField("newsLetterSubscription", StringType(), True),\
StructField("site", site, True),\
StructField("entries", ArrayType(entry), True),\
StructField("subtotal", DoubleType(), True),\
StructField("totalDiscount", DoubleType(), True),\
StructField("user", user, True),\
StructField("paymentInfo", payment_info, True),\
StructField("deliveryStatus", common_code, True),\
StructField("eventDate", StringType(), True),\
StructField("status", status, True),\
StructField("deliveryCost", DoubleType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #Write Functions

# COMMAND ----------

# DBTITLE 1,Merge
def saveToDeltaWithOverwrite(resultDf, silverLayerPath, database_name, target_table_name, mergeCol = None):
  if (mergeCol is None):
    if not DeltaTable.isDeltaTable(spark,f"{silverLayerPath}"):
      spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    resultDf.write.mode("overwrite").format("delta").option("path",silverLayerPath).saveAsTable(f"{database_name}.{target_table_name}")
  else:
    mappedCol = map((lambda x: f"old.{x} = new.{x} "),mergeCol)
    deltaTable = DeltaTable.forPath(spark, f"{silverLayerPath}")
    deltaTable.alias("old").merge(resultDf.alias("new"),mappedCol.mkString(" and "))\
    .whenMatched\
    .updateAll()\
    .whenNotMatched\
    .insertAll()\
    .execute()

# COMMAND ----------

# DBTITLE 1,Write function when mode is overwrite
#  Method to write to the datalake
#  @param  df  : dataframe
#  @param  year  : year column in dataframe
#  @param  month  : month column in dataframe
#  @param  day  : day column in dataframe
#  @param  path  : gold layer location
#  @param  database_name  : name of the database
#  @param  target_table_name  : name of the table
#  @return delta table


def overwrite_dataLake_with_delta_partition (df, year, month, day, path, table_name, database_name):
  spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
  df.write.partitionBy("year","month","day")\
          .format("delta")\
          .mode("overwrite")\
          .option("overwriteSchema","True")\
          .option("replaceWhere",f"year ={year} AND month = {month} AND day ={day}")\
          .option("path",path)\
          .saveAsTable("{}.{}".format(database_name, table_name))

# COMMAND ----------

# DBTITLE 1,Write to delta lake
#  Method to write to the datalake
#  @param  result_df  : dataframe
#  @param  silver_layer_path  : silverlayer location
#  @param  database_name  : name of the database
#  @param  target_table_name  : name of the table
#  @param  mode  : format to save the delta table
#  @return delta table

def write_to_deltalake(result_df, silver_layer_path, database_name, target_table_name):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    result_df.write.mode("overwrite").format("delta").option("overwriteSchema","true").option("path",silver_layer_path).saveAsTable(f"{database_name}.{target_table_name}")

# COMMAND ----------

def save_to_delta_with_overwrite(df, data_base_name, target_table_name, merge_col, silver_layer_path, partition_col):
    not_null_check = " & ".join(f"col('{col}').isNotNull()" for col in merge_col)
    
    result_df = df.dropDuplicates().filter(eval(not_null_check))

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if not DeltaTable.isDeltaTable(spark, silver_layer_path):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {data_base_name}")
        
        if len(partition_col) > 0:
            result_df.write.mode("overwrite").partitionBy(partition_col) \
                .format("delta") \
                .option("path", silver_layer_path) \
                .option("mergeSchema", True) \
                .saveAsTable(f"{data_base_name}.{target_table_name}")
        else:
            result_df.write.mode("overwrite") \
                .format("delta") \
                .option("path", silver_layer_path) \
                .option("mergeSchema", True) \
                .saveAsTable(f"{data_base_name}.{target_table_name}")
    else:
        delta_table = DeltaTable.forPath(spark, silver_layer_path)
        match_keys = " AND ".join(f"old.{col} = new.{col}" for col in merge_col)
        delta_table.alias("old") \
            .merge(result_df.alias("new"), match_keys) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()


# COMMAND ----------

def delta_overwrite(df, data_base_name, target_table_name, merge_col, silver_layer_path, partition_col):
    spark.sql(f'create database if not exists {data_base_name}')
    not_null_check = " & ".join(f"col('{col}').isNotNull()" for col in merge_col)
    
    result_df = df.dropDuplicates().filter(eval(not_null_check))

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    if len(partition_col) > 0:
      result_df.write.mode("overwrite").partitionBy(partition_col) \
              .format("delta") \
              .option("path", silver_layer_path)\
              .option("overwriteSchema", True) \
              .option("mergeSchema", True) \
              .saveAsTable(f"{data_base_name}.{target_table_name}")
    else:
      result_df.write.mode("overwrite") \
      .format("delta") \
      .option("path", silver_layer_path) \
      .option("mergeSchema", True) \
      .saveAsTable(f"{data_base_name}.{target_table_name}")

# COMMAND ----------

# DBTITLE 1,Remove special characters in columns
# Method to remove special character in the column name
#  @param  s  : string
#  @return list of columns

def remove_special_characters(s):
  return s.translate({ord(i):None for i in '.)'}).translate({ord(" "):'_'}).translate({ord("/"):'_'}).translate({ord("-"):'_'}).translate({ord("_"):'_'}).translate({ord("("):'_'})

# COMMAND ----------

# DBTITLE 1,Lower the Columns
# Method to lower the columns
#  @param  df  : dataframe
#  @return dataframe

def lower_columns(df):
    return df.toDF(*[c.lower() for c in df.columns])

# COMMAND ----------

# DBTITLE 1,Exit the notebook if condition is true
# This aims to check the count of records in both the dataframes and exits the notebook when the count is zero in any one of them
#  @param  df1,df2  : two dataframes

def check_and_exit_notebook(df1,df2):
    count1 = df1.count()
    count2 = df2.count()
    if count1 == 0 or count2 == 0:
      dbutils.notebook.exit("No Data")
    else:
      return (df1,df2)

# COMMAND ----------

# DBTITLE 1,Filter Null Values
def filter_null(df, columns, qurantine_path):
  check_null_values = " | ".join(f'(col("{c}").isNull())' for c in columns)
  filter_null = df.filter(eval(check_null_values))
  count_null = filter_null.count()
  if (count_null == 0):
    return df
  else:
    save_null_to_quarantine(filter_null, qurantine_path)
    df = df.filter(~eval(check_null_values))
    return df

# COMMAND ----------

# DBTITLE 1,Change of date format
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
def date_format_func(df,cols,date_format):
  for c in cols:
    df = df.withColumn(c, to_date(c, date_format).cast("date"))
  return df
