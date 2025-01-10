import sys
from airflow.models import TaskInstance
from airflow import settings
from airflow.models import DagBag

def push():
    run_id = sys.argv[1]
    
    dag_bag = DagBag()
    dag = dag_bag.get_dag("churn_pipeline")
    task = dag.get_task("push_task")
    
    session = settings.Session()
    ti = TaskInstance(task=task, run_id=run_id)
    ti.xcom_push(key="filename", value="haerin")
    
if __name__ == "__main__":
    push()