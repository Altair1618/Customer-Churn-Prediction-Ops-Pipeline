from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

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
    
    # push = BashOperator(
    #     task_id="push_task",
    #     bash_command=(
    #         "python /shared/scripts/push.py "
    #         '{{ run_id }}'
    #     ),
    #     dag=dag,
    # )
    
    # drift_detection = BashOperator(
    #     task_id="drift_monitoring",
    #     bash_command=(
    #         "python /shared/scripts/drift-detection.py "
    #         '{{ run_id }}'
    #     ),
    #     dag=dag,
    # )

    etl_task >> train_model
    # push >> drift_detection >> etl_task >> train_model

def check_drift(ti):
    psi_result = ti.xcom_pull(task_ids='drift_detection_task', key='psi_result')
    if psi_result == "true":
        return 'trigger_model_training'
    else:
        return 'no_drift_detected'

with DAG(dag_id='drift_detection_dag', schedule_interval='@daily') as dag:
    drift_detection_task = BashOperator(
        task_id='drift_detection_task',
        bash_command='python /shared/scripts/drift-detection.py {{ run_id }}',
        do_xcom_push=True
    )

    check_drift_task = BranchPythonOperator(
        task_id='check_drift',
        python_callable=check_drift,
    )

    trigger_model_training = TriggerDagRunOperator(
        task_id='trigger_model_training',
        trigger_dag_id='churn_pipeline'
    )

    no_drift_detected = BashOperator(
        task_id='no_drift_detected',
        bash_command='echo "No drift detected"',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    drift_detection_task >> check_drift_task >> [trigger_model_training, no_drift_detected]

with DAG(dag_id='drift_simulator_dag', schedule_interval='@daily') as dag:
    simulate_drift = BashOperator(
        task_id='simulate_drift',
        bash_command='python /shared/scripts/drift-simulator.py'
    )