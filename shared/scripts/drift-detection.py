import sys
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from airflow.models import TaskInstance
from airflow import settings
from airflow.models import DagBag
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

def calculate_psi(expected, actual, buckets=10):
    breakpoints = np.linspace(np.min(expected), np.max(expected), buckets + 1)

    expected_dist = np.histogram(expected, bins=breakpoints, density=True)[0]
    actual_dist = np.histogram(actual, bins=breakpoints, density=True)[0]
    
    expected_dist += 1e-6
    actual_dist += 1e-6
    
    expected_dist /= expected_dist.sum()
    actual_dist /= actual_dist.sum()
    
    psi = np.sum((expected_dist - actual_dist) * np.log(expected_dist / actual_dist))
    
    return psi

def run_drift_detection():
    run_id = sys.argv[1]
    
    dag_bag = DagBag()
    dag = dag_bag.get_dag("churn_pipeline")
    task = dag.get_task("push_task")
    
    session = settings.Session()
    ti = TaskInstance(task=task, run_id=run_id)
    value = ti.xcom_pull(key="filename")
    
    data_path = f"/shared/processed/{value}.parquet"
    data = pd.read_parquet(data_path)
    
    X = data.drop("Churn", axis=1)
    y = data["Churn"]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    new_data = pd.read_parquet(new_data_path)
    new_data = new_data[X_train.columns]
    
    
    currentTask = dag.get_task("drift_detection_task")
    
    # Iterate through numerical columns
    for col in X_train.select_dtypes(include=np.number).columns:
        psi = calculate_psi(X_train[col].values, new_data[col].values)
        if psi > 0.1: 
            # send true
            ti = TaskInstance(task=currentTask, run_id=run_id)
            ti.xcom_push(key="psi_result", value="true")
            break
    else:
        # send false
        ti = TaskInstance(task=currentTask, run_id=run_id)
        ti.xcom_push(key="psi_result", value="false")
    
    
if __name__ == "__main__":
    run_drift_detection()