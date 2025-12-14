CREATE TABLE IF NOT EXISTS `<Project_ID>.<DataSet>.ODP_ACCOUNTS_CDC` (
  account_id        STRING NOT NULL,
  balance           NUMERIC NOT NULL,
  as_of_date        DATE NOT NULL,
  rec_load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  as_of_date_time   TIMESTAMP NOT NULL,
  start_date        DATE NOT NULL,
  end_date          DATE NOT NULL,
  row_hash          STRING NOT NULL,
  is_current        BOOL NOT NULL,
  record_check_ts   TIMESTAMP     -- Last reconciliation timestamp
)
PARTITION BY start_date
CLUSTER BY account_id;
