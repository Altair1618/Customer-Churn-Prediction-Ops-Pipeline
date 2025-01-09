from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

with DAG(dag_id='churn_pipeline', start_date=days_ago(1), schedule_interval='@daily') as dag:
    etl_task = SparkSubmitOperator(
        task_id='run_etl',
        application='/shared/scripts/clean.py',
        conn_id='spark-conn'
    )

    train_model = SparkSubmitOperator(
        task_id='train_model',
        application='/shared/scripts/model.py',
        conn_id='spark-conn'
    )
    
    # drift_monitoring = BashOperator(
    #     task_id='drift_monitoring',
    #     bash_command=''
    # )

    # etl_task >> train_model >> drift_monitoring
    etl_task >> train_model
