from pyspark.sql import SparkSession


input_path = "/shared/data/customer_churn.csv"
output_path = "/shared/processed/customer_churn_cleaned.parquet"


def clean(input_path, output_path):
    spark = SparkSession \
        .builder \
        .appName("Customer Churn Data Cleaning") \
        .getOrCreate()
    
    data = spark.read.csv(input_path, header=True, inferSchema=True)

    data = data.dropna()
    data = data.dropDuplicates()

    data.write.parquet(output_path, mode="overwrite")


if __name__ == "__main__":
    clean(input_path, output_path)