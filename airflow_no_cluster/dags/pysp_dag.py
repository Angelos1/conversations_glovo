from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_pipeline_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

start_task = DummyOperator(task_id='start', dag=dag)

def run_spark_task():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("AirflowSparkTask") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Your Spark code here
    # Example: Create a DataFrame and print its schema
    data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)
    df.printSchema()

    spark.stop()

spark_task = PythonOperator(
    task_id='run_spark_task',
    python_callable=run_spark_task,
    dag=dag,
)

start_task >> spark_task
