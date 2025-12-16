expire_changed_current = f"""
MERGE `{gcp_project_id}.{target_cdc_dataset}.{table_name}_CDC` T
USING (
  SELECT
    account_id,
    TO_HEX(
      MD5(
        CONCAT(
          CAST(account_id AS STRING), '|',
          CAST(balance AS STRING), '|',
          CAST(as_of_date AS STRING)
        )
      )
    ) AS src_row_hash
  FROM `{gcp_project_id}.{staging_dataset_name}.{table_name}_STG`
) S
ON T.account_id = S.account_id
AND T.is_current = TRUE

WHEN MATCHED
AND T.row_hash != S.src_row_hash
THEN UPDATE SET
  is_current      = FALSE,
  end_date        = CURRENT_DATE(),
  record_check_ts = CURRENT_TIMESTAMP();
"""

new_changed_records = f"""
INSERT INTO `{gcp_project_id}.{target_cdc_dataset}.{table_name}_CDC` (
  account_id,
  balance,
  as_of_date,
  row_hash,
  is_current,
  start_date,
  end_date,
  record_check_ts
)
SELECT
  S.account_id,
  S.balance,
  S.as_of_date,
  TO_HEX(
    MD5(
      CONCAT(
        CAST(S.account_id AS STRING), '|',
        CAST(S.balance AS STRING), '|',
        CAST(S.as_of_date AS STRING)
      )
    )
  ) AS row_hash,
  TRUE                AS is_current,
  CURRENT_DATE()      AS start_date,
  NULL                AS end_date,
  CURRENT_TIMESTAMP() AS record_check_ts
FROM `{gcp_project_id}.{staging_dataset_name}.{table_name}_STG` S
LEFT JOIN `{gcp_project_id}.{target_cdc_dataset}.{table_name}_CDC` T
  ON S.account_id = T.account_id
 AND T.is_current = TRUE
WHERE
  T.account_id IS NULL
  OR T.row_hash != TO_HEX(
        MD5(
          CONCAT(
            CAST(S.account_id AS STRING), '|',
            CAST(S.balance AS STRING), '|',
            CAST(S.as_of_date AS STRING)
          )
        )
      );
"""

current_unchanged = f"""
UPDATE `{gcp_project_id}.{target_cdc_dataset}.{table_name}_CDC` T
SET record_check_ts = CURRENT_TIMESTAMP()
FROM `{gcp_project_id}.{staging_dataset_name}.{table_name}_STG` S
WHERE
  T.account_id = S.account_id
  AND T.is_current = TRUE
  AND T.row_hash = TO_HEX(
        MD5(
          CONCAT(
            CAST(S.account_id AS STRING), '|',
            CAST(S.balance AS STRING), '|',
            CAST(S.as_of_date AS STRING)
          )
      ));
"""

def run_scd2_merge(**context):
    ti = context["ti"]

    # Values already produced earlier in DAG
    job_start_ts = ti.xcom_pull(key="job_start_ts")
    table_name = ti.xcom_pull(key="table_name")

    gcp_project_id = Variable.get("gcp_project_id")
    staging_dataset_name = Variable.get("staging_dataset_name")
    target_cdc_dataset = Variable.get("target_cdc_dataset")
    gcp_conn_id = Variable.get("gcp_conn_id")
    impersonation_chain = Variable.get("impersonation_chain")
    bq_location = Variable.get("bq_location", default_var="EU")

    bq_hook = BigQueryHook(
        gcp_conn_id=gcp_conn_id,
        impersonation_chain=impersonation_chain,
        location=bq_location,
        use_legacy_sql=False,
    )

    client = bq_hook.get_client(project_id=gcp_project_id)

    client.query(expire_changed_current).result()
    client.query(new_changed_records).result()
    client.query(current_unchanged).result()

-- Source count
SELECT COUNT(*) AS staging_count
FROM `{gcp_project_id}.{staging_dataset_name}.{table_name}_STG`;

-- Target touched rows (CURRENT only)
SELECT COUNT(*) AS touched_current_rows
FROM `{gcp_project_id}.{target_cdc_dataset}.{table_name}_CDC`
WHERE is_current = TRUE
  AND record_check_ts >= TIMESTAMP('{job_start_ts}');