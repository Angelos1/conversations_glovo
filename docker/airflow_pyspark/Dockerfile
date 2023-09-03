FROM godatadriven/pyspark:latest

COPY ./airflow /opt/miniconda3/lib/python3.11/site-packages/airflow/example_dags

# Install Apache Airflow
RUN pip install --no-cache-dir apache-airflow

RUN mkdir -p ./input

RUN mkdir -p ./data_lake

RUN mkdir -p ./catalog

ADD ./start_airflow.sh .

ENTRYPOINT ["/start_airflow.sh"]
