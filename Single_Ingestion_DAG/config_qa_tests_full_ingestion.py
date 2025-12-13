from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery, storage
from datetime import datetime
import json

with DAG(
    dag_id="config_qa_tests_full_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    def run_qa(**context):
        record = {
            "dag_name": context["dag_run"].dag_id,
            "run_id": context["dag_run"].run_id,
            "created_timestamp": datetime.utcnow().isoformat(),
        }

        bucket = Variable.get("test_report_bucket")
        folder = Variable.get("test_report_folder")

        storage.Client().bucket(bucket).blob(
            f"{folder}/qa_{context['ds_nodash']}.json"
        ).upload_from_string(json.dumps(record))

        bigquery.Client(project=Variable.get("qa_bq_project")).insert_rows_json(
            f"{Variable.get('qa_bq_project')}.{Variable.get('qa_bq_dataset')}.{Variable.get('qa_bq_table')}",
            [record],
        )

    PythonOperator(task_id="run_qa", python_callable=run_qa)
