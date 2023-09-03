from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()



df = spark.read.option("multiline", "true") \
        .json("orders.json")

df.write.mode("overwrite").csv("orders.csv", header=True)  # Set header=True to include column names as the first row


df = spark.read.option("multiline", "true") \
        .json("customer_courier_chat_messages.json")

df.write.mode("overwrite").csv("customer_courier_chat_messages.csv", header=True)