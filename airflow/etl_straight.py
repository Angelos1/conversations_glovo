from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, when, first, col, min, row_number, year, month

from pyspark.sql import SparkSession

input_dir = "/data_lake/input"
output_dir = "/data_lake/output"

# filenames for the data files that are created in between the ETL steps
customer_courier_chat_messages_filename = "customer_courier_chat_messages"
customer_courier_chat_messages_enhanced_filename = "customer_courier_chat_messages_enhanced"
first_message_senders_filename = "first_message_senders"
first_responsetime_delays_filename = "first_responsetime_delays"
last_message_order_stage_filename = "last_message_order_stage"
aggregations_filename = "aggregations"
customer_courier_conversations_filename = "customer_courier_conversations"

def read_parquet(spark_session ,dir , filename) :
    return spark_session.read.parquet("{}/{}".format(dir, filename))



first_message_senders_filename = "first_message_senders"

spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'schedule_interval': '0 * * * *'
}

dag = DAG('udac_example_daggg',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


# -----------------------------------------------------------------------------------------------------------------------
def enhance_dataset():
    customer_courier_messages = spark.read.option("multiline", "true") \
        .json("{}/{}.json".format(input_dir, customer_courier_chat_messages_filename))

    customer_courier_messages_enhanced = customer_courier_messages \
        .withColumn("messageSentTimestamp", unix_timestamp("messageSentTime", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn("year", year("messageSentTime")) \
        .withColumn("month", month("messageSentTime"))

    customer_courier_messages_enhanced.write.option("header", True) \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet("{}/{}".format(output_dir, customer_courier_chat_messages_enhanced_filename))


enhance_dataset_operator = PythonOperator(
    task_id="Enhance_dataset",
    python_callable=enhance_dataset,
    dag=dag
)


# -----------------------------------------------------------------------------------------------------------------------

def find_first_message_senders():
    # required_fields_df = spark.read.parquet("{}/{}".format(output_dir, customer_courier_chat_messages_enhanced_filename)) \
    #     .select("orderId", "senderAppType", "chatStartedByMessage")
    required_fields_df = read_parquet(spark, output_dir, customer_courier_chat_messages_enhanced_filename) \
        .select("orderId", "senderAppType", "chatStartedByMessage")

    required_fields_df.createOrReplaceTempView("required_fields")

    # Identify the senders of the first messages for each order
    first_message_senders = spark.sql("""
        SELECT
            orderId,
            CASE WHEN senderAppType = 'Courier App' THEN 'courier' ELSE 'customer' END AS first_message_by
        FROM
            required_fields
        WHERE
            chatStartedByMessage = true
    """)

    first_message_senders.write.option("header", True) \
        .mode("overwrite") \
        .parquet("{}/{}".format(output_dir, first_message_senders_filename))


first_message_senders_operator = PythonOperator(
    task_id="First_message_senders",
    python_callable=find_first_message_senders,
    dag=dag
)


# -----------------------------------------------------------------------------------------------------------------------

def find_first_responsetime_delays():
    # finding first_responsetime_delay_seconds
    # required_fields_df = spark.read.parquet("{}/{}".format(output_dir, customer_courier_chat_messages_enhanced_filename)) \
    #     .select("orderId", "fromId", "toId", "messageSentTimestamp")
    required_fields_df = read_parquet(spark, output_dir, customer_courier_chat_messages_enhanced_filename) \
        .select("orderId", "fromId", "toId", "messageSentTimestamp")

    window_spec = Window.partitionBy("orderId").orderBy("messageSentTimestamp")

    response_time_delays_df = required_fields_df.withColumn("responsetime_delay_from_first",
                                                            when(col("fromId") != first("fromId").over(window_spec),
                                                                 col("messageSentTimestamp") - first(
                                                                     "messageSentTimestamp").over(
                                                                     window_spec)
                                                                 )
                                                            )
    first_response_time_delays = response_time_delays_df \
        .groupBy("orderId").agg(min("responsetime_delay_from_first").alias("first_responsetime_delay_seconds")) \
        .select("orderId", "first_responsetime_delay_seconds")

    first_response_time_delays.write.option("header", True) \
        .mode("overwrite") \
        .parquet("{}/{}".format(output_dir, first_responsetime_delays_filename))


first_responsetime_delays_operator = PythonOperator(
    task_id="First_responsetime_delays",
    python_callable=find_first_responsetime_delays,
    dag=dag
)


# -----------------------------------------------------------------------------------------------------------------------

def find_last_message_order_stage():
    # finding latest order stage
    # required_fields_df = spark.read.parquet("/data_lake/output/{}".format(customer_courier_chat_messages_enhanced_filename)) \
    #     .select("orderId", "orderStage", "messageSentTimestamp")
    required_fields_df = read_parquet(spark, output_dir, customer_courier_chat_messages_enhanced_filename)\
        .select("orderId", "orderStage", "messageSentTimestamp")

    window_spec = Window.partitionBy("orderId").orderBy(col("messageSentTimestamp").desc())

    ordered_by_stages = required_fields_df.withColumn("row_number", row_number().over(window_spec))

    ordered_by_stages.createOrReplaceTempView("ordered_by_stages")

    latest_order_stage = spark.sql("""
        SELECT
            orderId,
            orderStage as last_message_order_stage
        FROM
            ordered_by_stages
        WHERE
            row_number = 1
    """)

    latest_order_stage.write.option("header", True) \
        .mode("overwrite") \
        .parquet("{}/{}".format(output_dir, last_message_order_stage_filename))

last_message_order_stage_operator = PythonOperator(
    task_id="Last_message_order_stage",
    python_callable=find_last_message_order_stage,
    dag=dag
)
# -----------------------------------------------------------------------------------------------------------------------

def calculate_aggregate_fields():
    # calculating the final fields that can be calculated with an aggregation on the initial messages dataset
    # customer_courier_messages = spark.read.parquet("{}/{}".format(output_dir, customer_courier_chat_messages_enhanced_filename)) \

    customer_courier_messages = read_parquet(spark, output_dir, customer_courier_chat_messages_enhanced_filename)

    customer_courier_messages.createOrReplaceTempView("customer_courier_messages")

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

    aggregations.write.option("header", True) \
        .mode("overwrite") \
        .parquet("{}/{}".format(output_dir, aggregations_filename))

aggregate_fields_operator = PythonOperator(
    task_id="Aggregate_fields",
    python_callable=calculate_aggregate_fields,
    dag=dag
)

# -----------------------------------------------------------------------------------------------------------------------
def customer_courier_conversations_stats():

    # reading the nessesary files and creating the required pyspark temporary views for the final query
    aggregations = read_parquet(spark, output_dir, aggregations_filename)
    aggregations.createOrReplaceTempView("aggregations")

    orders = spark.read.option("multiline", "true").json("{}/{}.json".format(input_dir, "orders"))
    orders.createOrReplaceTempView("orders")

    first_message_senders = read_parquet(spark, output_dir, first_message_senders_filename)
    first_message_senders.createOrReplaceTempView("first_message_senders")

    first_responsetime_delays = read_parquet(spark, output_dir, first_responsetime_delays_filename)
    first_responsetime_delays.createOrReplaceTempView("first_responsetime_delays")

    last_message_stage = read_parquet(spark, output_dir, last_message_order_stage_filename)
    last_message_stage.createOrReplaceTempView("last_message_stage")

    final_query = """
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

    customer_courier_conversations_stats = spark.sql(final_query)

    customer_courier_conversations_stats.write.option("header", True) \
        .partitionBy("city_code") \
        .mode("overwrite") \
        .parquet("{}/{}".format(output_dir, customer_courier_conversations_filename))


customer_courier_conversations_operator = PythonOperator(
    task_id="Customer_courier_conversations",
    python_callable=customer_courier_conversations_stats,
    dag=dag
)

# -----------------------------------------------------------------------------------------------------------------------

# setting the DAG dependencies
enhance_dataset_operator >> [first_message_senders_operator,
                             first_responsetime_delays_operator,
                             last_message_order_stage_operator,
                             aggregate_fields_operator] >> customer_courier_conversations_operator
