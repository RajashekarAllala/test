CREATE TABLE IF NOT EXISTS `{{gcp_project_id}}.{{target_cdc_dataset}}.{{table_name}}_CDC` (
  account_id        STRING NOT NULL,
  balance           NUMERIC,
  as_of_date        DATE,

  -- audit
  rec_load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

  -- SCD Type-2 columns
  start_date        DATE NOT NULL,
  end_date          DATE,
  row_hash          STRING NOT NULL,
  is_current        BOOL NOT NULL,
  record_check_ts   TIMESTAMP


)
PARTITION BY DATE_TRUNC(start_date, MONTH)
CLUSTER BY account_id;