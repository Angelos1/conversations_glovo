from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, when, first, col, min, row_number, year, month

# Create a Spark session
spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()

# Load the datasets as temporary SQL tables
customer_courier_messages = spark.read.option("multiline", "true").json("data_sets/customer_courier_chat_messages.json")
orders = spark.read.option("multiline", "true").json("data_sets/orders.json")
print("-----------------------------", orders.count())

customer_courier_messages_enhanced = customer_courier_messages\
    .withColumn("messageSentTimestamp", unix_timestamp("messageSentTime", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("year", year("messageSentTime")) \
    .withColumn("month", month("messageSentTime"))

customer_courier_messages_enhanced.write.option("header", True) \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet("output/customer_courier_chat_messages_enhanced")


customer_courier_messages_enhanced.createOrReplaceTempView("customer_courier_messages")
orders.createOrReplaceTempView("orders")

customer_courier_messages = customer_courier_messages_enhanced



# ----------------------------------------------------------------------------------------------------------------------

# Identify the senders of the first messages for each order
first_message_senders = spark.sql("""
    SELECT
        orderId,
        CASE WHEN senderAppType = 'Courier App' THEN 'courier' ELSE 'customer' END AS first_message_by
    FROM
        customer_courier_messages
    WHERE
        chatStartedByMessage = true
""")

# Create a temporary view for the first message senders
first_message_senders.createOrReplaceTempView("first_message_senders")
# ----------------------------------------------------------------------------------------------------------------------




# ----------------------------------------------------------------------------------------------------------------------

# finding first_responsetime_delay_seconds
# req_fields_df = spark.read.option("multiline", "true").json("data_sets/customer_courier_chat_messages_enhanced.json")\
req_fields_df = spark.read.parquet("output/customer_courier_chat_messages_enhanced")\
    .select("orderId", "fromId", "toId", "messageSentTimestamp")

# req_fields_df = req_fields_df.withColumn("ts_sec", unix_timestamp("messageSentTime", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
window_spec = Window.partitionBy("orderId").orderBy("messageSentTimestamp")

# users_table = users_table.withColumn("row_number", row_number().over(window_spec))
rtime_delays_df = req_fields_df.withColumn("responsetime_delay_from_first",
                                           when(col("fromId") != first("fromId").over(window_spec),
                                                col("messageSentTimestamp") - first("messageSentTimestamp").over(window_spec)
                                                )
                                           )
first_rtime_delays = rtime_delays_df.groupBy("orderId").agg(min("responsetime_delay_from_first")
                                                               .alias("first_responsetime_delay_seconds")
                                                               ).select("orderId", "first_responsetime_delay_seconds")

first_rtime_delays.createOrReplaceTempView("first_responsetime_delays")
# ----------------------------------------------------------------------------------------------------------------------





# ----------------------------------------------------------------------------------------------------------------------

# finding max order
# req_fields_df = spark.read.option("multiline", "true").json("data_sets/customer_courier_chat_messages_enhanced.json")\
req_fields_df = spark.read.parquet("output/customer_courier_chat_messages_enhanced")\
    .select("orderId", "orderStage", "messageSentTimestamp")
# req_fields_df = req_fields_df.withColumn("ts_sec", unix_timestamp("messageSentTime", "yyyy-MM-dd'T'HH:mm:ss'Z'"))

window_spec2 = Window.partitionBy("orderId").orderBy(col("messageSentTimestamp").desc())

req_fields_df = req_fields_df.withColumn("row_number", row_number().over(window_spec2))

# print("-------------------", req_fields_df.columns)

req_fields_df.createOrReplaceTempView("max_orders_stages")

last_message_stage = spark.sql("""
    SELECT
        orderId,
        orderStage as last_message_order_stage
    FROM
        max_orders_stages
    WHERE
        row_number = 1
""")

last_message_stage.createOrReplaceTempView("last_message_stage")
# ----------------------------------------------------------------------------------------------------------------------





# ----------------------------------------------------------------------------------------------------------------------

aggregate_query = """
    SELECT
        ccm.orderId AS order_id,
        MIN(CASE WHEN ccm.senderAppType = 'Courier App' THEN ccm.messageSentTimestamp END) AS first_courier_message,
        MIN(CASE WHEN ccm.senderAppType = 'Customer iOS' THEN ccm.messageSentTimestamp END) AS first_customer_message,
        SUM(CASE WHEN ccm.senderAppType = 'Courier App' THEN 1 ELSE 0 END) AS num_messages_courier,
        SUM(CASE WHEN ccm.senderAppType = 'Customer iOS' THEN 1 ELSE 0 END) AS num_messages_customer,
        MIN(ccm.messageSentTimestamp) AS conversation_started_at,
        MAX(ccm.messageSentTimestamp) AS last_message_time,
        MAX(ccm.orderStage) AS last_message_order_stage
    FROM
        customer_courier_messages ccm
    GROUP BY
        ccm.orderId
"""

aggregations = spark.sql(aggregate_query)
aggregations.createOrReplaceTempView("aggregations")


query = """
    SELECT
        a.order_id AS order_id,
        o.cityCode AS city_code,
        a.first_courier_message AS first_courier_message,
        a.first_customer_message AS first_customer_message,
        a.num_messages_courier AS num_messages_courier,
        a.num_messages_customer AS num_messages_customer,
        fms.first_message_by AS first_message_by,
        a.conversation_started_at AS conversation_started_at,
        frd.first_responsetime_delay_seconds AS first_responsetime_delay_seconds,
        a.last_message_time AS last_message_time,
        lms.last_message_order_stage AS last_message_order_stage
    FROM
        aggregations a
    JOIN
        orders o
    ON
        a.order_id = o.orderId
    JOIN
        first_message_senders fms
    ON
        a.order_id = fms.orderId
    JOIN 
         first_responsetime_delays frd
    ON  
        a.order_id = frd.orderId 
    JOIN 
        last_message_stage lms
    ON     
        a.order_id = lms.orderId
    
"""


# Define the main SQL query
# query = """
#     SELECT
#         ccm.orderId AS order_id,
#         CASE WHEN o.cityCode IS NULL THEN 'N/A' else o.cityCode END AS city_code,
#         MIN(CASE WHEN ccm.senderAppType = 'Courier App' THEN ccm.messageSentTime END) AS first_courier_message,
#         MAX(CASE WHEN ccm.senderAppType = 'Customer iOS' THEN ccm.messageSentTime END) AS first_customer_message,
#         SUM(CASE WHEN ccm.senderAppType = 'Courier App' THEN 1 ELSE 0 END) AS num_messages_courier,
#         SUM(CASE WHEN ccm.senderAppType = 'Customer iOS' THEN 1 ELSE 0 END) AS num_messages_customer,
#         fms.first_message_by AS first_message_by,
#         MIN(ccm.messageSentTime) AS conversation_started_at,
#         frd.first_responsetime_delay_seconds AS first_responsetime_delay_seconds,
#         MAX(ccm.messageSentTime) AS last_message_time,
#         MAX(ccm.orderStage) AS last_message_order_stage
#     FROM
#         customer_courier_messages ccm
#     JOIN
#         orders o
#     ON
#         ccm.orderId = o.orderId
#     JOIN
#         first_message_senders fms
#     ON
#         ccm.orderId = fms.orderId
#     JOIN
#          first_responsetime_delays frd
#          on ccm.orderId = frd.orderId
#     GROUP BY
#         ccm.orderId, o.cityCode, fms.first_message_by
# """

# Execute the main SQL query
conversations_table = spark.sql(query)

# Write the output table to the Data Lake
# conversations_table.write.mode("overwrite").json("output/result.json")
conversations_table.write.mode("overwrite").csv("output/result.csv", header=True)  # Set header=True to include column names as the first row

conversations_table.write.option("header", True) \
        .partitionBy("city_code") \
        .mode("overwrite") \
        .parquet("output/result")

conversations_table.printSchema()
# Stop the Spark session
spark.stop()
