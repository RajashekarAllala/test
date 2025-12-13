from airflow.models import Variable
from google.cloud import bigquery
from datetime import datetime

# -------------------------------------------------
# BigQuery Client
# -------------------------------------------------
def get_bq_client():
    return bigquery.Client(project=Variable.get("gcp_project_id"))

# -------------------------------------------------
# Generic COUNT helper
# -------------------------------------------------
def get_count(table_fq, where_clause=None):
    client = get_bq_client()
    sql = f"SELECT COUNT(*) AS cnt FROM `{table_fq}`"
    if where_clause:
        sql += f" WHERE {where_clause}"
    return list(client.query(sql).result())[0].cnt

# -------------------------------------------------
# SCD Type-2 reconciliation metrics
# -------------------------------------------------
def scd2_metrics(cfg):
    client = get_bq_client()

    stg = cfg["stg_table"]
    tgt = cfg["target_table"]
    pk = cfg["pk_cols"][0]   # assuming single PK (extendable)

    staging_count = get_count(stg)

    new_sql = f"""
    SELECT COUNT(*) cnt
    FROM `{stg}` S
    LEFT JOIN `{tgt}` T
      ON S.{pk} = T.{pk}
     AND T.is_current = TRUE
    WHERE T.{pk} IS NULL
    """

    unchanged_sql = f"""
    SELECT COUNT(*) cnt
    FROM `{stg}` S
    JOIN `{tgt}` T
      ON S.{pk} = T.{pk}
     AND T.is_current = TRUE
     AND T.row_hash = S.row_hash
    """

    changed_sql = f"""
    SELECT COUNT(*) cnt
    FROM `{stg}` S
    JOIN `{tgt}` T
      ON S.{pk} = T.{pk}
     AND T.is_current = TRUE
     AND T.row_hash != S.row_hash
    """

    new_cnt = list(client.query(new_sql).result())[0].cnt
    unchanged_cnt = list(client.query(unchanged_sql).result())[0].cnt
    changed_cnt = list(client.query(changed_sql).result())[0].cnt

    reconciled = new_cnt + unchanged_cnt + changed_cnt

    audit_status = "PASS" if reconciled == staging_count else "FAIL"

    return {
        "staging_count": staging_count,
        "new_records": new_cnt,
        "unchanged_records": unchanged_cnt,
        "changed_records": changed_cnt,
        "audit_status": audit_status,
    }

# -------------------------------------------------
# Pipeline-level Audit Writer
# -------------------------------------------------
def write_pipeline_audit(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]
    cfg = ti.xcom_pull(key="cfg")

    client = get_bq_client()

    audit_project = Variable.get("gcp_project_id")
    audit_dataset = Variable.get("audit_dataset")
    audit_table = Variable.get("audit_table")

    load_type = cfg["load_type"].upper()
    staging_count = get_count(cfg["stg_table"])

    if load_type == "RELOAD":
        target_count = get_count(cfg["target_table"])
        mismatch = staging_count - target_count
        audit_status = "PASS" if mismatch == 0 else "FAIL"
        metrics = {}

    else:
        metrics = scd2_metrics(cfg)
        audit_status = metrics["audit_status"]
        target_count = get_count(cfg["target_table"], "is_current = TRUE")
        mismatch = (
            metrics["staging_count"]
            - (
                metrics["new_records"]
                + metrics["unchanged_records"]
                + metrics["changed_records"]
            )
        )

    failed_tasks = [
        t.task_id
        for t in dag_run.get_task_instances()
        if t.state == "failed"
    ]

    job_status = "FAILED" if failed_tasks else "SUCCESS"

    record = {
        "dag_name": context["dag"].dag_id,
        "run_id": dag_run.run_id,
        "table_name": cfg["table_name"],
        "load_type": load_type,
        "job_start_time": dag_run.start_date,
        "job_end_time": datetime.utcnow(),
        "job_status": job_status,
        "source_count": staging_count,
        "target_count": target_count,
        "mismatch": mismatch,
        "audit_status": audit_status,
        "new_records": metrics.get("new_records"),
        "unchanged_records": metrics.get("unchanged_records"),
        "changed_records": metrics.get("changed_records"),
        "created_timestamp": datetime.utcnow(),
    }

    client.insert_rows_json(
        f"{audit_project}.{audit_dataset}.{audit_table}",
        [record],
    )
