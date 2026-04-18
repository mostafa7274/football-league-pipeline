from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="football_league_pipeline",
    default_args=default_args,
    description="Calculate football standings with PySpark and store in MongoDB",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["football", "spark", "mongodb"],
) as dag:

    check_mongodb = BashOperator(
        task_id="check_mongodb_connection",
        bash_command=(
            "docker exec football_mongodb mongosh --eval "
            "\"db.adminCommand({ ping: 1 })\" && echo 'MongoDB OK'"
        ),
    )

    run_spark_job = BashOperator(
        task_id="calculate_standings_spark",
        bash_command=(
            "docker exec football_spark "
            "/opt/spark/bin/spark-submit "
            "--packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 "
            "--conf spark.jars.ivy=/tmp/.ivy2 "
            "/opt/spark/spark_jobs/calculate_standings.py"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    verify_mongo = BashOperator(
        task_id="verify_mongodb_results",
        bash_command=(
            "docker exec football_mongodb mongosh --eval "
            "\"var count = db.getSiblingDB('football_db').standings.countDocuments();"
            " print('Documents in MongoDB: ' + count);\""
        ),
    )

    check_mongodb >> run_spark_job >> verify_mongo