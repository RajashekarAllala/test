CREATE TABLE IF NOT EXISTS `{{project_id}}.{{target_cdc_dataset}}.target_cdc` (
  account_id        STRING NOT NULL,
  balance           NUMERIC NOT NULL,
  as_of_date        DATE NOT NULL,
  rec_load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  as_of_date_time   TIMESTAMP NOT NULL,
  row_hash          STRING NOT NULL,
  start_date        DATE NOT NULL,
  end_date          DATE NOT NULL,
  is_current        BOOL NOT NULL,
  record_check_ts   TIMESTAMP     -- Last reconciliation timestamp
)
PARTITION BY start_date
CLUSTER BY account_id;
