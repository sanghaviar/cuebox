# Databricks notebook source
# DBTITLE 1,Required Import(s)
# MAGIC %run ./orders_utils

# COMMAND ----------

# DBTITLE 1,Read Parameters from the user
dbutils.widgets.text("schedule_date", "", "schedule_date")
dbutils.widgets.text("data_lake_base_uri", "", "data_lake_base_uri")
dbutils.widgets.text("database_name", "", "database_name")
dbutils.widgets.text("target_table_name","orders")

data_lake_base_uri = dbutils.widgets.get("data_lake_base_uri")
database_name = dbutils.widgets.get("database_name")
target_table_name = dbutils.widgets.get("target_table_name")
schedule_date = dbutils.widgets.get("schedule_date")

bronze_layer_path = data_lake_base_uri +f'/bronze/cue-box/{target_table_name}/*'
silver_layer_path = data_lake_base_uri + f'/silver/cue-box/{target_table_name}/'

# COMMAND ----------

# DBTITLE 1,Constant(s)
OPTIONS_DICT = {"multiline" : "false"}


MERGE_COLUMNS = ["order_number", "entries_entry_number", "event_date", "entries_product_code"]

PARTITION_COLUMNS = ["country_code"]

SELECT_COLUMNS = ["code","entries_entryNumber","eventDate","status_code","trackingUrl","salesOrderNumber","sapGoodsIssueDate","sapPlantCode","trackingId","orderCreatedDateInECC","creationTime","totalPrice","totalDiscounts","totalTax","orderPlacedDate","deliveryNumber","productRegistration","csCode","bundleIds","servicesSubTotalWithoutDiscounts","paymentStatus","appliedCoupons","servicesSubTotal","utmDetails","newsLetterSubscription","subtotal","totalDiscount","deliveryCost","deliveryAddress_isBusinessUser","deliveryAddress_town","deliveryAddress_email","consignments_shippingDate","consignments_trackingID","consignments_code","consignments_deliveredDate","consignments_namedDeliveryDate","consignments_statusDisplay","fsChannel_name","fsChannel_code","fsChannel_salesChannelType_code","currency_isoCode","site_uid","entries_quantity","entries_totalPrice","entries_isFocusedPrice","entries_aService","entries_d2cPrice","entries_productInfo","entries_basePrice","entries_discountName","entries_percentageTax","entries_totalTax","entries_discountRows","user_uid","user_referredByCode","user_referredByStaffName","paymentInfo_code","paymentInfo_adyenPaymentMethodName","paymentInfo_adyenPaymentMethod","deliveryStatus_code","entries_product_code","entries_parentEntry_entryNumber","entries_order_code","user_type_code","allPromotionResults_promotion_code","cart_level_promotions", "country","load_date"]


NEW_COLUMN_NAMES = ["order_number","entries_entry_number","event_date","order_status_code","tracking_url","sales_order_number","sap_goods_issue_date","sap_plant_code","tracking_id","order_created_date_in_ecc","creation_time","total_price","total_discounts","total_tax","order_placed_date","delivery_number","product_registration","cs_code","bundle_ids","services_sub_total_without_discounts","payment_status","applied_coupons","services_sub_total","utm_details","news_letter_subscription","order_subtotal","order_total_discount","delivery_cost","delivery_address_is_business_user","delivery_address_town","delivery_address_email","consigments_shipping_date","consigments_tracking_id","consigments_code","consigments_delivered_date","consigments_named_delivery_date","consigments_status_display","fs_channel_name","fs_channel_code","fs_channel_sales_channel_type_code","currency_iso_code","site_uid","entries_quantity","entries_total_price","entries_is_focused_price","entries_a_service","entries_d2c_price","entries_product_info","entries_base_price","entries_discount_name","entries_percentage_tax","entries_total_tax","entries_discount_rows","user_uid","user_referred_by_code","user_referred_by_staff_name","payment_info_code","payment_info_adyen_payment_method_name","payment_info_adyen_payment_method","delivery_status_code","entries_product_code","entries_parent_entry_entry_number","entries_order_code","user_type_code","entries_promotion_code","cart_level_promotions","country_code","load_date"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## EXTRACT

# COMMAND ----------

# DBTITLE 1,Read Orders Data - Bronze Layer
order_data_with_file_name = read_json_from_datalake(bronze_layer_path, OPTIONS_DICT).filter("entries is not null")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TRANSFORM

# COMMAND ----------

# DBTITLE 1, Order Data
# Take the latest details of an consigment
order_data = order_data_with_file_name.withColumn("consignments", col("consignments")[size("consignments") -1])

# Keep promotions related information in one DF along with order code
# And flatten it seprately
order_data_promotions = order_data.select("code", "allPromotionResults", "eventDate")

# Keep order details excluding promotion details in one DF
# And faltten it seprately
order_data_entries = order_data.drop("allPromotionResults")

# COMMAND ----------

# DBTITLE 1,Promotion Data
# Flatten Promotion Data - by exploding allPromotionResults Array<Structure>
flatten_order_promotions = flatten_json_df(order_data_promotions).withColumn("allPromotionResults_consumedEntries", explode_outer(split("allPromotionResults_consumedEntries", "\\|")))

# Cart Level Promotions - Promotions which do not have any consumed entries
# For an order collect all such promotions in an array
cart_level_promotions = flatten_order_promotions.filter((col("allPromotionResults_consumedEntries") == '') | ((col("allPromotionResults_consumedEntries").isNull())) & (col("allPromotionResults_promotion_code").isNotNull())).groupBy("code").agg(collect_set("allPromotionResults_promotion_code").alias("cart_level_promotions"))

# Combine normal promotions and cart level promotions
all_promotions = flatten_order_promotions.filter("allPromotionResults_consumedEntries != '' or allPromotionResults_consumedEntries is null")
promotion_df = all_promotions.withColumnRenamed("allPromotionResults_consumedEntries","entries_entryNumber")

# There can be multiple promotions attached to single order entry, after explode they will be in different rows. But as Requested by business merging them into one
promotion_final_df = promotion_df.select("code", "eventDate", "entries_entryNumber", "allPromotionResults_promotion_code")\
                                .groupBy("code", "eventDate", "entries_entryNumber").agg(concat_ws('|', collect_list("allPromotionResults_promotion_code")).alias("allPromotionResults_promotion_code"))

# COMMAND ----------

# DBTITLE 1,Entries
flatten_order_entries = flatten_json_df(order_data_entries)

# COMMAND ----------

# DBTITLE 1,Join Promotion and Entries
# Add Promotion Information to Orders
complete_order_information_1 = flatten_order_entries.join(promotion_final_df, ["code", "entries_entryNumber","eventDate"], "left")

# Add Cart-Level Information to Orders
complete_order_information = complete_order_information_1.join(cart_level_promotions, ["code"], "left")

# COMMAND ----------

# DBTITLE 1,Convert Date Column(s) to Readable Format 
fix_dates = complete_order_information.withColumn("creationTime", epoch_to_timestamp_udf("creationTime"))\
                            .withColumn("orderPlacedDate", epoch_to_timestamp_udf("orderPlacedDate"))\
                            .withColumn("orderCreatedDateInECC", epoch_to_timestamp_udf("orderCreatedDateInECC"))\
                            .withColumn("consignments_deliveredDate", epoch_to_timestamp_udf("consignments_deliveredDate"))\
                            .withColumn("consignments_namedDeliveryDate", epoch_to_timestamp_udf("consignments_namedDeliveryDate"))\
                            .withColumn("consignments_shippingDate", epoch_to_timestamp_udf("consignments_shippingDate"))\
                            .withColumn("eventDate", epoch_to_timestamp_udf("eventDate"))\
                            .withColumn("sapGoodsIssueDate", epoch_to_timestamp_udf("sapGoodsIssueDate"))\
                            .withColumn("country", when(col("site_uid").isNotNull(),split(col("site_uid"), "-")[0]).otherwise(lit(None)))

# COMMAND ----------

# DBTITLE 1,Add Audit Date
add_audit_date = fix_dates.withColumn("load_date", lit(schedule_date))

# COMMAND ----------

# DBTITLE 1,Change Column Name(s)
final_df = add_audit_date.select(*SELECT_COLUMNS).toDF(*NEW_COLUMN_NAMES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LOAD

# COMMAND ----------

final_df.write.mode('overwrite').format('delta').option('overwriteSchema',True).option('path',silver_layer_path).saveAsTable('ccv2_europe.orders_test')

# COMMAND ----------

# DBTITLE 1,Load Data to Table
save_to_delta_with_overwrite(final_df, database_name, target_table_name, MERGE_COLUMNS, silver_layer_path, PARTITION_COLUMNS)
