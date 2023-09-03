from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()

df = spark.read.parquet("/Users/angelosprastitis/PycharmProjects/conversations_glovo/data_lake/output/2023-09-03/customer_courier_conversations")

df.write.mode("overwrite").csv("customer_courier_conversations222.csv", header=True)  # Set header=True to include column names as the first row
