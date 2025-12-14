CREATE TABLE IF NOT EXISTS `<Project_ID>.<DataSet>.ODP_ACCOUNTS` (
  account_id        STRING NOT NULL,
  balance           NUMERIC NOT NULL,
  as_of_date        DATE NOT NULL,
  rec_load_ts       TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  as_of_date_time   TIMESTAMP NOT NULL,
  record_check_ts   TIMESTAMP
);
