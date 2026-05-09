# Big Data Banking Pipeline

## Project Overview

This project is a Big Data pipeline for analyzing banking transactions using PySpark, Docker, and Apache Airflow.

## Technologies Used

* PySpark
* Apache Airflow
* Docker
* Python

## Features

* Read banking transaction dataset
* Analyze transaction types
* Detect fraud transactions
* Find top cities
* Save results automatically
* Automate workflow using Airflow

## Project Structure

* `spark_jobs/` → PySpark analysis scripts
* `airflow/dags/` → Airflow DAGs
* `data/` → Dataset
* `output/` → Generated results

## How to Run

### Start Docker Containers

```bash
docker compose up -d
```

### Run Spark Analysis

```bash
python spark_jobs/spark_analysis.py
```

### Open Airflow

http://localhost:8081

## Output Example

Generated CSV file:

```text
output/top_cities.csv
```
