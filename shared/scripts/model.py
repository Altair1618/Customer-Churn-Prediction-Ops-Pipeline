import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split


data_path = "/shared/processed/customer_churn_cleaned.parquet"


def train_model(data_path):
    data = pd.read_parquet(data_path)
    
    X = data.drop("Churn", axis=1)
    y = data["Churn"]
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    mlflow.log_metric("accuracy", accuracy)
    mlflow.sklearn.log_model(model, "costumer_churn_model")


if __name__ == "__main__":
    mlflow.set_tracking_uri("http://mlflow-server:5000")
    mlflow.set_experiment("Customer Churn Model Training")
    with mlflow.start_run():
        train_model(data_path)