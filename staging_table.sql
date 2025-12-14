CREATE TABLE IF NOT EXISTS `<Project_ID>.<DataSet>.ODP_ACCOUNTS_STG` (
  account_id        STRING NOT NULL,
  balance           NUMERIC NOT NULL,
  as_of_date        DATE NOT NULL,
  rec_load_ts       TIMESTAMP NOT NULL,
  as_of_date_time   TIMESTAMP
);


-- Insert 10 records
INSERT INTO `<Project_ID>.<DataSet>.ODP_ACCOUNTS_STG`
(
  account_id,
  balance,
  as_of_date,
  rec_load_ts,
  as_of_date_time
)
SELECT
  CONCAT('ACC', LPAD(CAST(id AS STRING), 8, '0')) AS account_id,
  CAST(1000 + id * 250 AS NUMERIC) AS balance,
  DATE_ADD(DATE '2025-01-01', INTERVAL id - 1 DAY) AS as_of_date,
  CURRENT_TIMESTAMP() AS rec_load_ts,
  TIMESTAMP_ADD(
    TIMESTAMP '2025-01-01 09:00:00',
    INTERVAL id - 1 DAY
  ) AS as_of_date_time
FROM UNNEST(GENERATE_ARRAY(1, 10)) AS id;
