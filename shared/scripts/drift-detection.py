import sys
from airflow.models import TaskInstance
from airflow import settings
from airflow.models import DagBag

def run_drift_detection():
    run_id = sys.argv[1]
    
    dag_bag = DagBag()
    dag = dag_bag.get_dag("churn_pipeline")
    task = dag.get_task("push_task")
    
    session = settings.Session()
    ti = TaskInstance(task=task, run_id=run_id)
    value = ti.xcom_pull(key="filename")
    
    print(f"Value: {value}")
    
if __name__ == "__main__":
    run_drift_detection()