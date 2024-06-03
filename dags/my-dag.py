from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def trigger_first_dbt_job():
    url = "https://ji129.us1.dbt.com/api/v2/accounts/70403103926902/jobs/70403103937325/run/"
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer dbtc_vphQr7kdfuJBtW33uIUzdbEIB7xivCDHOrIXwVf-5aqwZkltys",
        "Content-Type": "application/json"
    }
    data = {
        "cause": "Triggered via API"
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 200:
        print("Request was successful")
    else:
        print(f"Failed to trigger job: {response.status_code}, {response.text}")


def trigger_second_dbt_job():
    url = "https://ji129.us1.dbt.com/api/v2/accounts/70403103926902/jobs/70403103937326/run/"
    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer dbtc_vphQr7kdfuJBtW33uIUzdbEIB7xivCDHOrIXwVf-5aqwZkltys",
        "Content-Type": "application/json"
    }
    data = {
        "cause": "Triggered via API"
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    if response.status_code == 200:
        print("Request was successful")
    else:
        print(f"Failed to trigger job: {response.status_code}, {response.text}")


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'paper_dbt_job',
    default_args=default_args,
    description='A simple DAG to trigger a dbt Cloud job',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Define the task using PythonOperator
trigger_dbt_job_task = PythonOperator(
    task_id='trigger_first_dbt_job',
    python_callable=trigger_first_dbt_job,
    dag=dag,
)

# Set the task in the DAG
trigger_dbt_job_task 
