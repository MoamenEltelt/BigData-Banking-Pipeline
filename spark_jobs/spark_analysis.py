from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder \
    .appName("Banking Big Data Analysis") \
    .getOrCreate()

# Read CSV Dataset
df = spark.read.csv(
    "/data/Banking_Transactions_USA_2023_2024.csv",
    header=True,
    inferSchema=True
)

print("===== Dataset Preview =====")
df.show(5)

print("===== Dataset Schema =====")
df.printSchema()

# =========================
# Top Cities
# =========================

print("===== Top Cities =====")

top_cities = df.groupBy("City") \
    .count() \
    .orderBy(col("count").desc())

top_cities.show()

# =========================
# Top Categories
# =========================

print("===== Top Categories =====")

top_categories = df.groupBy("Category") \
    .count() \
    .orderBy(col("count").desc())

top_categories.show()

# =========================
# Fraud Transactions
# =========================

print("===== Fraud Transactions =====")

fraud_transactions = df.groupBy("Fraud_Flag") \
    .count()

fraud_transactions.show()

# =========================
# Transaction Types
# =========================

print("===== Transaction Types =====")

transaction_types = df.groupBy("Transaction_Type") \
    .count()

transaction_types.show()

# =========================
# Average Transaction Amount
# =========================

print("===== Average Transaction Amount =====")

avg_amount = df.selectExpr(
    "avg(Transaction_Amount) as average_amount"
)

avg_amount.show()

# =========================
# Save CSV Files
# =========================

top_cities.toPandas().to_csv(
    "/output/top_cities.csv",
    index=False
)

top_categories.toPandas().to_csv(
    "/output/top_categories.csv",
    index=False
)

fraud_transactions.toPandas().to_csv(
    "/output/fraud_transactions.csv",
    index=False
)

transaction_types.toPandas().to_csv(
    "/output/transaction_types.csv",
    index=False
)

avg_amount.toPandas().to_csv(
    "/output/average_transaction_amount.csv",
    index=False
)

print("Results saved successfully!")

# Stop Spark
spark.stop()