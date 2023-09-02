from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, when, first, col, min, row_number, year, month

# Create a Spark session
spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()

# Load the datasets as temporary SQL tables
customer_courier_messages = spark.read.option("multiline", "true").json("../data_sets/customer_courier_chat_messages.json")

customer_courier_messages_enhanced = customer_courier_messages\
    .withColumn("messageSentTimestamp", unix_timestamp("messageSentTime", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
    .withColumn("year", year("messageSentTime")) \
    .withColumn("month", month("messageSentTime"))

customer_courier_messages_enhanced.show()
#
# customer_courier_messages_enhanced.write.option("header", True) \
#         .partitionBy("year", "month") \
#         .mode("overwrite") \
#         .parquet("result")

df = spark.read.parquet("result")
df.show()