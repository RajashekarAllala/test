CREATE TABLE IF NOT EXISTS `{{project_id}}.{{target_reload_dataset}}.target_reload` (
  account_id        STRING NOT NULL,
  balance           NUMERIC NOT NULL,
  as_of_date        DATE NOT NULL,
  rec_load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  as_of_date_time   TIMESTAMP NOT NULL,
  row_hash          STRING,
  record_check_ts   TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
