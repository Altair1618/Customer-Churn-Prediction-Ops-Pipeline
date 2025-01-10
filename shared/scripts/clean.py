from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import IntegerType, FloatType


input_path = "/shared/data/customer_churn_old.csv"
output_path = "/shared/processed/customer_churn_cleaned.parquet"

def convert_to_binary(datum: str) -> int:
    if datum == 'No':
        return 0  
    return 1

def convert_to_service_code(datum: str) -> int:
    if datum == 'No':
        return 0
    if datum == 'Yes':
        return 1
    return 2

def clean(input_path, output_path):
    spark = SparkSession \
        .builder \
        .appName("Customer Churn Data Cleaning") \
        .getOrCreate()
    
    data = spark.read.csv(input_path, header=True, inferSchema=True)

    data = data.drop("customerID")
    data = data.dropna().dropDuplicates()

    data = data.withColumn('gender', when(col('gender') == 'Female', 0).otherwise(1))

    data = data.withColumn('MultipleLines', when(col('MultipleLines') == 'No', 0)
                                        .when(col('MultipleLines') == 'Yes', 1)
                                        .otherwise(2))

    data = data.withColumn('InternetService', when(col('InternetService') == 'No', 0)
                                         .when(col('InternetService') == 'DSL', 1)
                                         .otherwise(2))

    data = data.withColumn('PaymentMethod', when(col('PaymentMethod') == 'Bank transfer (automatic)', 0)
                                        .when(col('PaymentMethod') == 'Credit card (automatic)', 1)
                                        .when(col('PaymentMethod') == 'Electronic check', 2)
                                        .otherwise(3))

    data = data.withColumn('Contract', when(col('Contract') == 'Month-to-month', 0)
                                    .when(col('Contract') == 'One year', 1)
                                    .otherwise(2))

    data = data.withColumn('MonthlyCharges', col('MonthlyCharges').cast(FloatType()))
    data = data.withColumn('TotalCharges', col('TotalCharges').cast(FloatType()))

    binary_columns = ['Partner', 'Dependents', 'PhoneService', 'PaperlessBilling', 'Churn']

    for column in binary_columns:
        data = data.withColumn(column, binary_udf(col(column)))

    trinary_columns = ['OnlineSecurity', 'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies']

    for column in trinary_columns:
        data = data.withColumn(column, service_code_udf(col(column)))

    data = data.dropna().dropDuplicates()
    data.write.parquet(output_path, mode="overwrite")


if __name__ == "__main__":
    binary_udf = udf(convert_to_binary, IntegerType())
    service_code_udf = udf(convert_to_service_code, IntegerType())

    clean(input_path, output_path)