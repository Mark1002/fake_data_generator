"""BQ client."""
from google.cloud import bigquery

client = bigquery.Client()

dataset_ref = client.dataset('for_pratice', 'tw-rd-de-mark-chang')
table_ref = dataset_ref.table('social_media_partition')
social_media_partition_table = client.get_table(table_ref)
df = client.list_rows(social_media_partition_table, max_results=10).to_dataframe()

print(df)
