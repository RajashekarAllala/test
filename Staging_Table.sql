CREATE TABLE IF NOT EXISTS `<Project_ID>.<DataSet>.ODP_ACCOUNTS_STG` (
  account_id        STRING NOT NULL,
  balance           NUMERIC NOT NULL,
  as_of_date        DATE NOT NULL,
  rec_load_ts       TIMESTAMP NOT NULL,
  as_of_date_time   TIMESTAMP
);
