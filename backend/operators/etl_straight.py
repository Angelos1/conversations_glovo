from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import unix_timestamp, when, first, col, min, row_number, year, month

from pyspark.sql import SparkSession

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


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)


def enhance_dataset():
    customer_courier_messages = spark.read.option("multiline", "true").json("/data_lake/input/customer_courier_chat_messages.json")

    customer_courier_messages_enhanced = customer_courier_messages \
        .withColumn("messageSentTimestamp", unix_timestamp("messageSentTime", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
        .withColumn("year", year("messageSentTime")) \
        .withColumn("month", month("messageSentTime"))

    customer_courier_messages_enhanced.write.option("header", True) \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet("/data_lake/output/customer_courier_chat_messages_enhanced")


enhance_dataset_operator = PythonOperator(
   task_id="enh",
   python_callable=enhance_dataset,
   dag=dag
)

start_operator >> enhance_dataset_operator