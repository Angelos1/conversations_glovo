from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.enhance_dataset import EnhanceDatasetOperator

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("ConversationAggregation").master("local").getOrCreate()

# from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
#                                 LoadDimensionOperator, DataQualityOperator)
# from helpers import SqlQueries

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

enhance_messages_dataset = EnhanceDatasetOperator(
    task_id='Enhance_messages_dataset',
    dag=dag,
    spark_session=spark,
    json_path='../data_sets/customer_courier_chat_messages.json',
    output_path='output'
)

start_operator >> enhance_messages_dataset
