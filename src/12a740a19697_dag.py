from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator


# Notebook path on Databricks to run
notebook_task = {
    'notebook_path': '/Users/<user_email>/Batch_data_cleaning_&_querying' 
}

#Define params for Run Now Operator
notebook_params = { 
    "Variable":5
}

# Airflow DAG's default arguments
default_args = {
    'owner': '12a740a19697',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2)
}

# Creating a DAG that will run daily.
with DAG(dag_id = '12a740a19697_dag',
        start_date = datetime(2024,1,30),
        schedule_interval = '@daily',
        catchup = False,
        default_args = default_args
    ) as dag:

    # Task that the DAG will perform, running a Databricks notebook.    
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='<cluster_id>',
        notebook_task=notebook_task
    )
    
    opr_submit_run