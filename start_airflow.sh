#!/bin/bash

# Initialize the Airflow database (if not already done)
airflow db init 

# Create a user
airflow users create --username glovo --password glovo --firstname glovo --lastname glovo --role Admin --email admin@example.com

# Start the Airflow scheduler in the background
airflow scheduler &

# Start the Airflow web server in the foreground
airflow webserver -p 8080
