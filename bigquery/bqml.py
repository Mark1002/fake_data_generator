"""BQML."""
from google.cloud import bigquery

client = bigquery.Client()
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
# training model
query = """
CREATE OR REPLACE MODEL `for_pratice.sample_model` 
OPTIONS(model_type='logistic_reg') AS
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20160801' AND '20170631'
LIMIT 100000;
"""
query_job = client.query(query, job_config=safe_config)
query_job.result()
# predict model
query = """
SELECT
  *
FROM
  ml.EVALUATE(MODEL `for_pratice.sample_model`, (
SELECT
  IF(totals.transactions IS NULL, 0, 1) AS label,
  IFNULL(device.operatingSystem, "") AS os,
  device.isMobile AS is_mobile,
  IFNULL(geoNetwork.country, "") AS country,
  IFNULL(totals.pageviews, 0) AS pageviews
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170701' AND '20170801'));
"""
query_job = client.query(query, job_config=safe_config)
result_df = query_job.to_dataframe()

print(result_df)
