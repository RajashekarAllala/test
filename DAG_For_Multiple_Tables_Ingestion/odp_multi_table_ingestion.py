from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from datetime import datetime
import json
import re
from pathlib import Path
import sys

DAG_DIR = str(Path(__file__).resolve().parent)

if DAG_DIR not in sys.path:
    sys.path.insert(0, DAG_DIR)

from reusable_fiwcon_templates.myAudit import write_audit

# =====================================================
# Airflow Variables
# =====================================================
GCP_CONN_ID = Variable.get("gcp_conn_id")
IMPERSONATION_CHAIN = Variable.get("impersonation_chain")

PROJECT_ID = Variable.get("gcp_project_id")

CONFIG_DATASET = Variable.get("config_dataset_name")
CONFIG_TABLE = Variable.get("config_table_name")

STAGING_DATASET = Variable.get("staging_dataset_name")
TARGET_RELOAD_DATASET = Variable.get("target_reload_dataset")
TARGET_CDC_DATASET = Variable.get("target_cdc_dataset")

DQ_BUCKET = Variable.get("dq_config_bucket")
DQ_OBJECT = Variable.get("dq_config_object")

ARCHIVE_BUCKET = Variable.get("archival_bucket")
ARCHIVE_PATH = Variable.get("archival_path")

QA_DAG_ID = Variable.get("qa_dag_id")

# =====================================================
# Hooks
# =====================================================
def bq():
    return BigQueryHook(
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN
    )

def gcs():
    return GCSHook(
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN
    )

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="odp_multi_table_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["odp", "ingestion"]
) as dag:

    # =================================================
    # 1. Read ACTIVE config rows
    # =================================================
    @task
    def read_config_table():
        dag_name = dag.dag_id
        t_start = datetime.utcnow()

        try:
            rows = [
                dict(r) for r in bq().get_records(
                    f"""
                    SELECT *
                    FROM `{PROJECT_ID}.{CONFIG_DATASET}.{CONFIG_TABLE}`
                    WHERE is_active = TRUE
                    """
                )
            ]

            if not rows:
                raise AirflowFailException("No active rows found in config table")

            write_audit(
                dag_name, "ALL", "read_config_table",
                "read_config_table",
                t_start, datetime.utcnow(),
                "SUCCESS",
                source_count=len(rows)
            )
            return rows

        except Exception as e:
            write_audit(
                dag_name, "ALL", "read_config_table",
                "read_config_table",
                t_start, datetime.utcnow(),
                "FAILED",
                error_message=str(e)
            )
            raise

    # =================================================
    # 2. Per-table ingestion
    # =================================================
    @task
    def ingest_table(cfg: dict):
        dag_name = dag.dag_id
        table_name = cfg["table_name"]
        load_type = cfg["load_type"].upper()
        task_id = "ingest_table"

        stg_table = f"{PROJECT_ID}.{STAGING_DATASET}.{table_name}_STG"
        job_start_ts = datetime.utcnow()

        # -------------------------------------------------
        # Load DQ config
        # -------------------------------------------------
        t_start = datetime.utcnow()
        try:
            dq_cfg = json.loads(
                gcs().download(DQ_BUCKET, DQ_OBJECT).decode("utf-8")
            )

            if stg_table not in dq_cfg:
                raise AirflowFailException(f"DQ config missing for {stg_table}")

            rules = dq_cfg[stg_table]
            pk_cols = rules["pk_cols"]
            not_null_cols = rules.get("not_null_cols", [])

            write_audit(
                dag_name, table_name, task_id,
                "load_dq_config",
                t_start, datetime.utcnow(), "SUCCESS"
            )
        except Exception as e:
            write_audit(
                dag_name, table_name, task_id,
                "load_dq_config",
                t_start, datetime.utcnow(),
                "FAILED", error_message=str(e)
            )
            raise

        # -------------------------------------------------
        # Footer validation (single GCS read)
        # -------------------------------------------------
        t_start = datetime.utcnow()
        try:
            content = gcs().download(
                cfg["gcs_bucket"], cfg["file_name"]
            ).decode("utf-8")

            footer_line = content.strip().splitlines()[-1]

            match = re.search(
                rules["footer_line_regex"],
                footer_line,
                re.IGNORECASE
            )

            if not match:
                raise AirflowFailException("Footer regex did not match")

            footer_count = int(match.group(1))

            write_audit(
                dag_name, table_name, task_id,
                "validate_footer_and_get_count",
                t_start, datetime.utcnow(), "SUCCESS"
            )
        except Exception as e:
            write_audit(
                dag_name, table_name, task_id,
                "validate_footer_and_get_count",
                t_start, datetime.utcnow(),
                "FAILED", error_message=str(e)
            )
            raise

        # -------------------------------------------------
        # Load GCS → Staging
        # -------------------------------------------------
        t_start = datetime.utcnow()
        try:
            bq().run_load(
                destination_project_dataset_table=stg_table,
                source_uris=[f"gs://{cfg['gcs_bucket']}/{cfg['file_name']}"],
                source_format="CSV",
                skip_leading_rows=1,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE"
            )

            staging_count = bq().get_first(
                f"SELECT COUNT(*) FROM `{stg_table}`"
            )[0]

            if staging_count != footer_count:
                raise AirflowFailException(
                    f"Footer={footer_count}, staging={staging_count}"
                )

            write_audit(
                dag_name, table_name, task_id,
                "load_gcs_to_staging",
                t_start, datetime.utcnow(),
                "SUCCESS",
                source_count=footer_count,
                target_count=staging_count
            )
        except Exception as e:
            write_audit(
                dag_name, table_name, task_id,
                "load_gcs_to_staging",
                t_start, datetime.utcnow(),
                "FAILED", error_message=str(e)
            )
            raise

        # -------------------------------------------------
        # DQ checks
        # -------------------------------------------------
        t_start = datetime.utcnow()
        try:
            for col in not_null_cols:
                cnt = bq().get_first(
                    f"SELECT COUNT(*) FROM `{stg_table}` WHERE {col} IS NULL"
                )[0]
                if cnt > 0:
                    raise AirflowFailException(f"NOT NULL failed: {col}")

            pk_expr = ", ".join(pk_cols)
            dupes = bq().get_first(
                f"""
                SELECT COUNT(*) FROM (
                  SELECT {pk_expr}, COUNT(*) c
                  FROM `{stg_table}`
                  GROUP BY {pk_expr}
                  HAVING c > 1
                )
                """
            )[0]

            if dupes > 0:
                raise AirflowFailException("Duplicate primary keys detected")

            write_audit(
                dag_name, table_name, task_id,
                "dq_checks",
                t_start, datetime.utcnow(), "SUCCESS"
            )
        except Exception as e:
            write_audit(
                dag_name, table_name, task_id,
                "dq_checks",
                t_start, datetime.utcnow(),
                "FAILED", error_message=str(e)
            )
            raise

        # -------------------------------------------------
        # Load to Target + Reconciliation
        # -------------------------------------------------
        t_start = datetime.utcnow()
        try:
            if load_type == "RELOAD":
                target_table = f"{PROJECT_ID}.{TARGET_RELOAD_DATASET}.{table_name}"

                bq().run(
                    f"""
                    CREATE OR REPLACE TABLE `{target_table}` AS
                    SELECT *, CURRENT_TIMESTAMP() AS record_check_ts
                    FROM `{stg_table}`
                    """,
                    use_legacy_sql=False
                )

                tgt_count = bq().get_first(
                    f"SELECT COUNT(*) FROM `{target_table}`"
                )[0]

                if tgt_count != staging_count:
                    raise AirflowFailException(
                        f"RELOAD mismatch: staging={staging_count}, target={tgt_count}"
                    )

            else:
                target_table = f"{PROJECT_ID}.{TARGET_CDC_DATASET}.{table_name}"
                pk = pk_cols[0]

                bq().run(
                    f"""
                    MERGE `{target_table}` T
                    USING (
                      SELECT
                        S.*,
                        TO_HEX(MD5(TO_JSON_STRING(S))) AS row_hash
                      FROM `{stg_table}` S
                    ) S
                    ON T.{pk} = S.{pk}
                    AND T.is_current = TRUE

                    WHEN MATCHED AND T.row_hash = S.row_hash THEN
                      UPDATE SET
                        record_check_ts = CURRENT_TIMESTAMP()

                    WHEN MATCHED AND T.row_hash != S.row_hash THEN
                      UPDATE SET
                        is_current = FALSE,
                        end_date = CURRENT_DATE()

                    WHEN NOT MATCHED THEN
                      INSERT (
                        {pk},
                        row_hash,
                        start_date,
                        end_date,
                        is_current,
                        record_check_ts
                      )
                      VALUES (
                        S.{pk},
                        S.row_hash,
                        CURRENT_DATE(),
                        DATE '9999-12-31',
                        TRUE,
                        CURRENT_TIMESTAMP()
                      )
                    """,
                    use_legacy_sql=False
                )

                touched = bq().get_first(
                    f"""
                    SELECT COUNT(*)
                    FROM `{target_table}`
                    WHERE is_current = TRUE
                      AND record_check_ts >= TIMESTAMP('{job_start_ts}')
                    """
                )[0]

                if touched != staging_count:
                    raise AirflowFailException(
                        f"SCD2 mismatch: staging={staging_count}, touched={touched}"
                    )

            write_audit(
                dag_name, table_name, task_id,
                "load_to_target",
                t_start, datetime.utcnow(),
                "SUCCESS",
                source_count=staging_count,
                target_count=staging_count
            )

        except Exception as e:
            write_audit(
                dag_name, table_name, task_id,
                "load_to_target",
                t_start, datetime.utcnow(),
                "FAILED", error_message=str(e)
            )
            raise

        # -------------------------------------------------
        # Archive file
        # -------------------------------------------------
        t_start = datetime.utcnow()
        try:
            archive_object = f"{ARCHIVE_PATH}/{cfg['file_name'].split('/')[-1]}"

            gcs().copy(
                cfg["gcs_bucket"],
                cfg["file_name"],
                ARCHIVE_BUCKET,
                archive_object
            )
            gcs().delete(cfg["gcs_bucket"], cfg["file_name"])

            write_audit(
                dag_name, table_name, task_id,
                "archive_file",
                t_start, datetime.utcnow(), "SUCCESS"
            )
        except Exception as e:
            write_audit(
                dag_name, table_name, task_id,
                "archive_file",
                t_start, datetime.utcnow(),
                "FAILED", error_message=str(e)
            )
            raise

    cfgs = read_config_table()
    ingest_table.expand(cfg=cfgs)

    TriggerDagRunOperator(
        task_id="trigger_qa_dag",
        trigger_dag_id=QA_DAG_ID,
        trigger_rule="all_done"
    )
