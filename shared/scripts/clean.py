from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer


input_path = "/shared/data/customer_churn.csv"
output_path = "/shared/processed/customer_churn_cleaned.parquet"


def clean(input_path, output_path):
    spark = SparkSession \
        .builder \
        .appName("Customer Churn Data Cleaning") \
        .getOrCreate()
    
    data = spark.read.csv(input_path, header=True, inferSchema=True)

    data = data.drop("customerID")
    data = data.dropna().dropDuplicates()

    categorical_columns = [col for col, dtype in data.dtypes if dtype == "string"]
    for col in categorical_columns:
        indexer = StringIndexer(inputCol=col, outputCol=f"{col}_index")
        data = indexer.fit(data).transform(data)
        data = data.drop(col)

    data.write.parquet(output_path, mode="overwrite")


if __name__ == "__main__":
    clean(input_path, output_path)