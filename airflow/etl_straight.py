from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, when, first, col, min, row_number, year, month
from pyspark.sql import SparkSession
from datetime import datetime, date, timedelta
import logging
import os
import json

# execution date of the pipeline
date_today = date.today()

input_dir = "/data_lake/input"
output_dir = "/data_lake/output/{}".format(date_today)

# filenames for the data files that are created in between the ETL steps
customer_courier_chat_messages_filename = "customer_courier_chat_messages"
customer_courier_chat_messages_enhanced_filename = "customer_courier_chat_messages_enhanced"
first_message_senders_filename = "first_message_senders"
first_responsetime_delays_filename = "first_responsetime_delays"
last_message_order_stage_filename = "last_message_order_stage"
aggregations_filename = "aggregations"
customer_courier_conversations_filename = "customer_courier_conversations"

def read_parquet(spark_session, dir, filename):
    return spark_session.read.parquet("{}/{}".format(dir, filename))

first_message_senders_filename = "first_message_senders"

spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()

default_args = {
    'owner': 'Glovo',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email_on_retry': True,
}

dag = DAG('conversations_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime.now(),
          schedule_interval="@monthly"
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


# -----------------------------------------------------------------------------------------------------------------------
# Code block for enhance_dataset_operator
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

# -----------------------------------------------------------------------------------------------------------------------
# Code block for first_message_senders_operator

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

# -----------------------------------------------------------------------------------------------------------------------
# Code block for first_responsetime_delays_operator

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

# -----------------------------------------------------------------------------------------------------------------------
# Code block for last_message_order_stage_operator

def find_last_message_order_stage():
    # finding latest order stage
    # required_fields_df = spark.read.parquet("/data_lake/output/{}".format(customer_courier_chat_messages_enhanced_filename)) \
    #     .select("orderId", "orderStage", "messageSentTimestamp")
    required_fields_df = read_parquet(spark, output_dir, customer_courier_chat_messages_enhanced_filename) \
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

# -----------------------------------------------------------------------------------------------------------------------
# Code block for aggregate_fields_operator

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

# -----------------------------------------------------------------------------------------------------------------------
# Code block for customer_courier_conversations_operator

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

# -----------------------------------------------------------------------------------------------------------------------
# Code block for num_orders_quality_check_operator

def num_orders_quality_check():
    customer_courier_conversations = read_parquet(spark, output_dir, customer_courier_conversations_filename)
    customer_courier_conversations.createOrReplaceTempView("customer_courier_conversations")

    customer_courier_chat_messages = \
        read_parquet(spark, output_dir, customer_courier_chat_messages_enhanced_filename)
    customer_courier_chat_messages.createOrReplaceTempView("customer_courier_chat_messages")

    count_orders_in_conversations_dataset = spark.sql("""
        SELECT
            count(order_id)
        FROM
            customer_courier_conversations
        
    """).collect()[0][0]

    count_orders_in_messages_dataset = spark.sql("""
            SELECT
                count( DISTINCT orderId)
            FROM
                customer_courier_chat_messages
                
        """).collect()[0][0]

    if count_orders_in_conversations_dataset != count_orders_in_messages_dataset:
        raise ValueError("Number of unique orderIds in the initial dataset ({}) not "
                         "equal to the number of orders in the resulting dataset ({})"
                         .format(count_orders_in_messages_dataset, count_orders_in_conversations_dataset))

    logging.info("Data quality check on the number of records on the final set passed: {}"
                 .format(count_orders_in_conversations_dataset))


num_orders_quality_check_operator = PythonOperator(
    task_id="Number_orders_quality_check",
    python_callable=num_orders_quality_check,
    dag=dag
)

# -----------------------------------------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------------------------------------
# Code block for create_catalog_operator

def create_catalog():
    def create_directory_structure_json(path):
        result = {
            "name": os.path.basename(path),
            "type": "directory",
            "children": []
        }

        if os.path.isdir(path):
            for item in os.listdir(path):
                item_path = os.path.join(path, item)
                if os.path.isdir(item_path):
                    result["children"].append(create_directory_structure_json(item_path))
                else:
                    file_type = os.path.splitext(item)[1][1:]
                    if file_type in ["json", "parquet"]:
                        result["children"].append({
                            "name": item,
                            "type": os.path.splitext(item)[1][1:]
                        })

        return result

    directory_path = '/data_lake'

    if os.path.exists(directory_path) and os.path.isdir(directory_path):
        directory_structure_json = create_directory_structure_json(directory_path)

        # Write the JSON to a file
        with open('/catalog/data_lake_catalog.json', 'w') as json_file:
            json.dump(directory_structure_json, json_file, indent=4)
        print("JSON structure saved to 'directory_structure.json'")
    else:
        print(f"The directory '{directory_path}' does not exist.")


create_catalog_operator = PythonOperator(
    task_id="Create_catalog",
    python_callable=create_catalog,
    dag=dag
)
# -----------------------------------------------------------------------------------------------------------------------

tasks_to_be_executed_in_parallel = [first_message_senders_operator,
                                    first_responsetime_delays_operator,
                                    last_message_order_stage_operator,
                                    aggregate_fields_operator]

# setting the DAG dependencies
start_operator >> enhance_dataset_operator >> tasks_to_be_executed_in_parallel >> \
customer_courier_conversations_operator >> num_orders_quality_check_operator >> create_catalog_operator