from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "finsight",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

SPARK_SUBMIT = (
    "docker exec finsight-spark-master "
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "--packages org.postgresql:postgresql:42.7.3 "
)

with DAG(
    dag_id="spark_medallion_pipeline",
    description="Bronze → Silver → Gold Medallion Architecture",
    default_args=default_args,
    schedule="0 22 * * 1-5", # it will run every weekday at 22:00 (10 PM)
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["finsight", "spark", "medallion"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=SPARK_SUBMIT + "/opt/spark_jobs/bronze_to_silver.py",
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=SPARK_SUBMIT + "/opt/spark_jobs/silver_to_gold.py",
    )
    
    # Εγκαθιστά protobuf fix και τρέχει dbt run + dbt test
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "docker exec finsight-dbt pip install 'protobuf==4.25.3' -q && "
            "docker exec finsight-dbt dbt run && "
            "docker exec finsight-dbt dbt test"
        ),
    )

    bronze_to_silver >> silver_to_gold >> dbt_run