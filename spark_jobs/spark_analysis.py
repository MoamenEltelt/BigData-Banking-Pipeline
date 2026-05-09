from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Banking Big Data Analysis") \
    .getOrCreate()

df = spark.read.csv(
    "./data/Banking_Transactions_USA_2023_2024.csv",
    header=True,
    inferSchema=True
)

print("===== Dataset Preview =====")
df.show(5)

print("===== Dataset Schema =====")
df.printSchema()

print("===== Transaction Type Count =====")
df.groupBy("Transaction_Type").count().show()

print("===== Average Transaction Amount =====")
df.selectExpr("avg(Transaction_Amount)").show()

print("===== Top Cities =====")

top_cities = df.groupBy("City") \
    .count() \
    .orderBy(col("count").desc())

top_cities.show()

print("===== Top Categories =====")

df.groupBy("Category") \
    .count() \
    .orderBy(col("count").desc()) \
    .show()

print("===== Fraud Transactions =====")

df.groupBy("Fraud_Flag") \
    .count() \
    .show()

top_cities.toPandas().to_csv(
    "output/top_cities.csv",
    index=False
)

print("Results saved successfully!")

spark.stop()