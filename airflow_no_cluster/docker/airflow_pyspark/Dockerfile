FROM godatadriven/pyspark:latest

COPY ./dags /opt/miniconda3/lib/python3.11/site-packages/airflow/example_dags

# Install Apache Airflow
RUN pip install --no-cache-dir apache-airflow

RUN mkdir -p ./data_lake

RUN mkdir -p ./data_lake/input

RUN mkdir -p ./data_lake/output

ADD ./start_airflow.sh .

ENTRYPOINT ["/start_airflow.sh"]
