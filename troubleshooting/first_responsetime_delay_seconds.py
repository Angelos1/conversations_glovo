
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, when, first, col, min, row_number

# Create a Spark session
spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()

customer_courier_messages = spark.read.option("multiline", "true").json("../data_sets/customer_courier_chat_messages.json")

req_fields_df = spark.read.option("multiline", "true").json("../data_sets/customer_courier_chat_messages.json")\
    .select("orderId", "fromId", "toId", "messageSentTime")
req_fields_df = req_fields_df.withColumn("ts_sec", unix_timestamp("messageSentTime", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
window_spec = Window.partitionBy("orderId").orderBy("ts_sec")

# users_table = users_table.withColumn("row_number", row_number().over(window_spec))
rtime_delays_df = req_fields_df.withColumn("responsetime_delay_from_first",
                                           when(col("fromId") != first("fromId").over(window_spec),
                                                col("ts_sec") - first("ts_sec").over(window_spec)
                                                )
                                           )

req_fields_df.show()

rtime_delays_df.show()


first_rtime_delays = rtime_delays_df.groupBy("orderId").agg(min("responsetime_delay_from_first")
                                                               .alias("first_responsetime_delay_seconds")
                                                               ).select("orderId", "first_responsetime_delay_seconds")


first_rtime_delays.show()

# first_rtime_delays.createOrReplaceTempView("first_responsetime_delays")