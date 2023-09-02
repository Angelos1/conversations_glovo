from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, when, first, col

# Create a Spark session
spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()

# customer_courier_messages = spark.read.json("customer_courier_chat_messages.json").cache()

df = spark.read.option("multiline", "true").json('customer_courier_chat_messages.json')

# Show the contents of the DataFrame
df.show()