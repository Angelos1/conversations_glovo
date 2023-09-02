from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
from pyspark.sql.functions import unix_timestamp, year, month

class EnhanceDatasetOperator(BaseOperator):
    ui_color = '#358140'

    enhancing_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
    """

    # @apply_defaults
    def __init__(self,
                 spark_session,
                 json_path='',
                 output_path='',
                 *args, **kwargs):
        super(EnhanceDatasetOperator, self).__init__(*args, **kwargs)
        self.spark_session = spark_session
        self.json_path = json_path
        self.output_path = output_path

    def execute(self, context):
        self.log.info('Staging for {} table starting...'.format(self.table))

        self.log.info('Enhancing dataset...'.format(self.table))
        spark = self.spark_session
        customer_courier_messages = spark.read.option("multiline", "true").json(self.json_path)

        customer_courier_messages_enhanced = customer_courier_messages \
            .withColumn("messageSentTimestamp", unix_timestamp("messageSentTime", "yyyy-MM-dd'T'HH:mm:ss'Z'")) \
            .withColumn("year", year("messageSentTime")) \
            .withColumn("month", month("messageSentTime"))

        customer_courier_messages_enhanced.write.option("header", True) \
            .partitionBy("year", "month") \
            .mode("overwrite") \
            .parquet(self.output_path + "/customer_courier_chat_messages_enhanced")
