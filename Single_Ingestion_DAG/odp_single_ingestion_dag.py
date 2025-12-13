from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import json, re

from reusable_templates.myAudit import (
    get_bq_client,
    get_count,
    write_pipeline_audit,
)

with DAG(
    dag_id="odp_combined_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=90),
) as dag:

    GCP_CONN_ID = Variable.get("gcp_conn_id")
    PROJECT_ID = Variable.get("gcp_project_id")
    BQ_LOCATION = Variable.get("bq_location")

    CONFIG_DATASET = Variable.get("config_dataset_name")
    CONFIG_TABLE = Variable.get("config_table_name")
    STAGING_DATASET = Variable.get("staging_dataset_name")

    TARGET_RELOAD_DATASET = Variable.get("target_reload_dataset")
    TARGET_CDC_DATASET = Variable.get("target_cdc_dataset")

    DQ_BUCKET = Variable.get("dq_config_bucket")
    DQ_OBJECT = Variable.get("dq_config_object")

    ARCHIVAL_BUCKET = Variable.get("archival_bucket")
    ARCHIVAL_PATH = Variable.get("archival_path")

    QA_DAG_ID = Variable.get("qa_dag_id")
    TABLE_NAME = Variable.get("table_name")

    CONFIG_TABLE_FQ = f"{PROJECT_ID}.{CONFIG_DATASET}.{CONFIG_TABLE}"

    # ------------------ Read Config ------------------
    def read_config(**context):
        client = get_bq_client()
        sql = f"SELECT * FROM `{CONFIG_TABLE_FQ}` WHERE table_name='{TABLE_NAME}'"
        rows = list(client.query(sql).result())
        if not rows:
            raise AirflowFailException("Config not found")

        cfg = dict(rows[0].items())
        cfg["stg_table"] = f"{PROJECT_ID}.{STAGING_DATASET}.{TABLE_NAME}_STG"

        if cfg["load_type"].upper() == "RELOAD":
            cfg["target_table"] = f"{PROJECT_ID}.{TARGET_RELOAD_DATASET}.{TABLE_NAME}"
        else:
            cfg["target_table"] = f"{PROJECT_ID}.{TARGET_CDC_DATASET}.{TABLE_NAME}"

        context["ti"].xcom_push(key="cfg", value=cfg)

    t_read_config = PythonOperator(
        task_id="read_config_table",
        python_callable=read_config,
    )

    # ------------------ Load DQ JSON ------------------
    def load_dq(**context):
        cfg = context["ti"].xcom_pull(key="cfg")
        gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
        dq_all = json.loads(gcs.download(DQ_BUCKET, DQ_OBJECT))
        dq_cfg = dq_all[cfg["stg_table"]]
        cfg["pk_cols"] = dq_cfg["pk_cols"]
        context["ti"].xcom_push(key="cfg", value=cfg)
        context["ti"].xcom_push(key="dq_cfg", value=dq_cfg)

    t_load_dq = PythonOperator(
        task_id="load_dq_config",
        python_callable=load_dq,
    )

    # ------------------ Footer Validation ------------------
    def validate_footer(**context):
        ti = context["ti"]
        cfg = ti.xcom_pull(key="cfg")
        dq_cfg = ti.xcom_pull(key="dq_cfg")

        regex = dq_cfg["footer_line_regex"]
        gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
        text = gcs.download(cfg["gcs_bucket"], cfg["file_name"]).decode()
        footer = [l for l in text.splitlines() if l.strip()][-1]

        match = re.search(regex, footer, re.IGNORECASE)
        if not match:
            raise AirflowFailException("Footer validation failed")

        ti.xcom_push(key="footer_count", value=int(match.group(1)))

    t_footer = PythonOperator(
        task_id="validate_footer_and_get_count",
        python_callable=validate_footer,
    )

    t_truncate = BigQueryInsertJobOperator(
        task_id="truncate_staging_table",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        location=BQ_LOCATION,
        configuration={"query": {"query": "TRUNCATE TABLE `{{ ti.xcom_pull(key='cfg')['stg_table'] }}`", "useLegacySql": False}},
    )

    t_load_stage = GCSToBigQueryOperator(
        task_id="load_gcs_to_staging",
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        bucket="{{ ti.xcom_pull(key='cfg')['gcs_bucket'] }}",
        source_objects=["{{ ti.xcom_pull(key='cfg')['file_name'] }}"],
        destination_project_dataset_table="{{ ti.xcom_pull(key='cfg')['stg_table'] }}",
        source_format="{{ ti.xcom_pull(key='cfg')['file_type'] }}",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    def record_count_check(**context):
        cfg = context["ti"].xcom_pull(key="cfg")
        if get_count(cfg["stg_table"]) != context["ti"].xcom_pull(key="footer_count"):
            raise AirflowFailException("Count mismatch")

    t_count = PythonOperator(
        task_id="record_count_check",
        python_callable=record_count_check,
    )

    # ------------------ Load Target ------------------
    def load_target(**context):
        cfg = context["ti"].xcom_pull(key="cfg")
        dq_cfg = context["ti"].xcom_pull(key="dq_cfg")
        pk = dq_cfg["pk_cols"][0]
        non_keys = [c for c in dq_cfg["not_null_cols"] if c != pk]

        hash_expr = " || '|' || ".join([f"COALESCE(CAST({c} AS STRING),'')" for c in non_keys])

        if cfg["load_type"].upper() == "RELOAD":
            sql = f"""
            CREATE OR REPLACE TABLE `{cfg['target_table']}` AS
            SELECT *, TO_HEX(MD5({hash_expr})) row_hash,
                   CURRENT_TIMESTAMP() record_check_ts
            FROM `{cfg['stg_table']}`
            """
        else:
            sql = f"""
            MERGE `{cfg['target_table']}` T
            USING (SELECT *, TO_HEX(MD5({hash_expr})) row_hash FROM `{cfg['stg_table']}`) S
            ON T.{pk}=S.{pk} AND T.is_current=TRUE
            WHEN MATCHED AND T.row_hash=S.row_hash THEN
              UPDATE SET record_check_ts=CURRENT_TIMESTAMP()
            WHEN MATCHED AND T.row_hash!=S.row_hash THEN
              UPDATE SET is_current=FALSE, end_date=CURRENT_DATE()
            WHEN NOT MATCHED THEN
              INSERT ROW
            """

        BigQueryInsertJobOperator(
            task_id="bq_load_target",
            gcp_conn_id=GCP_CONN_ID,
            project_id=PROJECT_ID,
            location=BQ_LOCATION,
            configuration={"query": {"query": sql, "useLegacySql": False}},
        ).execute(context=context)

    t_load_target = PythonOperator(
        task_id="load_to_target",
        python_callable=load_target,
    )

    t_audit = PythonOperator(
        task_id="run_audit",
        python_callable=write_pipeline_audit,
    )

    t_archive = GCSToGCSOperator(
        task_id="archive_file",
        gcp_conn_id=GCP_CONN_ID,
        source_bucket="{{ ti.xcom_pull(key='cfg')['gcs_bucket'] }}",
        source_object="{{ ti.xcom_pull(key='cfg')['file_name'] }}",
        destination_bucket=ARCHIVAL_BUCKET,
        destination_object=f"{ARCHIVAL_PATH}/{{{{ ds_nodash }}}}",
        move_object=True,
    )

    t_trigger_qa = TriggerDagRunOperator(
        task_id="trigger_qa_dag",
        trigger_dag_id=QA_DAG_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        t_read_config
        >> t_load_dq
        >> t_footer
        >> t_truncate
        >> t_load_stage
        >> t_count
        >> t_load_target
        >> t_audit
        >> t_archive
        >> t_trigger_qa
    )
