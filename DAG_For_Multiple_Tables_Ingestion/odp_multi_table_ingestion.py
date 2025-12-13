from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
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
    dag_id="odp_multi_table_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=120),
    tags=["odp", "multi_table"],
) as dag:

    # --------------------------------------------------
    # Variables
    # --------------------------------------------------
    PROJECT_ID = Variable.get("gcp_project_id")
    GCP_CONN_ID = Variable.get("gcp_conn_id")
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

    CONFIG_TABLE_FQ = f"{PROJECT_ID}.{CONFIG_DATASET}.{CONFIG_TABLE}"

    # --------------------------------------------------
    # 1️⃣ Get active tables
    # --------------------------------------------------
    @task
    def get_active_tables():
        client = get_bq_client()
        sql = f"""
        SELECT *
        FROM `{CONFIG_TABLE_FQ}`
        WHERE is_active = TRUE
        """
        return [dict(r.items()) for r in client.query(sql).result()]

    # --------------------------------------------------
    # 2️⃣ Per-table ingestion
    # --------------------------------------------------
    @task
    def ingest_one_table(cfg):
        gcs = GCSHook(gcp_conn_id=GCP_CONN_ID)
        client = get_bq_client()

        # ---- derive tables
        cfg["stg_table"] = f"{PROJECT_ID}.{STAGING_DATASET}.{cfg['table_name']}_STG"
        if cfg["load_type"].upper() == "RELOAD":
            cfg["target_table"] = f"{PROJECT_ID}.{TARGET_RELOAD_DATASET}.{cfg['table_name']}"
        else:
            cfg["target_table"] = f"{PROJECT_ID}.{TARGET_CDC_DATASET}.{cfg['table_name']}"

        # ---- load DQ config
        dq_all = json.loads(gcs.download(DQ_BUCKET, DQ_OBJECT))
        dq_cfg = dq_all[cfg["stg_table"]]
        cfg["pk_cols"] = dq_cfg["pk_cols"]

        # ---- footer validation
        text = gcs.download(cfg["gcs_bucket"], cfg["file_name"]).decode()
        footer = [l for l in text.splitlines() if l.strip()][-1]
        match = re.search(dq_cfg["footer_line_regex"], footer, re.IGNORECASE)
        if not match:
            raise AirflowFailException(f"Footer validation failed for {cfg['table_name']}")
        footer_count = int(match.group(1))

        # ---- truncate staging
        client.query(f"TRUNCATE TABLE `{cfg['stg_table']}`").result()

        # ---- load staging
        GCSToBigQueryOperator(
            task_id=f"load_stage_{cfg['table_name']}",
            gcp_conn_id=GCP_CONN_ID,
            project_id=PROJECT_ID,
            bucket=cfg["gcs_bucket"],
            source_objects=[cfg["file_name"]],
            destination_project_dataset_table=cfg["stg_table"],
            source_format=cfg["file_type"],
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
        ).execute({})

        # ---- count check
        if get_count(cfg["stg_table"]) != footer_count:
            raise AirflowFailException("Source vs staging count mismatch")

        # ---- load target
        pk = cfg["pk_cols"][0]
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
        client.query(sql).result()

        # ---- audit
        write_pipeline_audit(cfg, dag_run=dag.get_dagrun(), dag=dag)

        # ---- archive
        gcs.copy(
            cfg["gcs_bucket"],
            cfg["file_name"],
            ARCHIVAL_BUCKET,
            f"{ARCHIVAL_PATH}/{cfg['table_name']}/{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        )
        gcs.delete(cfg["gcs_bucket"], cfg["file_name"])

        return cfg["table_name"]

    # --------------------------------------------------
    # DAG wiring
    # --------------------------------------------------
    active_cfgs = get_active_tables()
    ingest_one_table.expand(cfg=active_cfgs)

    TriggerDagRunOperator(
        task_id="trigger_qa_dag",
        trigger_dag_id=QA_DAG_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
