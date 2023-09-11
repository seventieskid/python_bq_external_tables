from google.cloud import bigquery
from google.cloud import storage
import os
import pandas as pd
import pandas_gbq
import uuid

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/garethrees/Downloads/k8s-play-unique-e5b5cf98bbc7.json"

BUCKET_NAME = "k8s-play-unique-fotc-cfs"

sql = """
    SELECT * FROM `DATAHUB.covid19_italy_national_trends`
"""

configuration = {
   'query': {
     "useQueryCache": False
   }
}

df = pandas_gbq.read_gbq(sql, configuration=configuration)
df['date'] = pd.to_datetime(df['date'])
df['note'] = df['note'].astype(str)
df.set_index(df['date'], inplace=True)
df_daily_groups = df.groupby(pd.Grouper(freq='D'))    #Group by day

client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)

for name, group in df_daily_groups:
    
    strName = name.strftime('%Y-%m-%d')

    # Create a filename based on the group name
    filename = f'{strName}.parquet'
    
    # Write the group to a CSV file
    group.to_parquet(filename, compression='gzip', index=False)

    blob = bucket.blob(f'covid19_italy_national_trends_with_hps/hps_date={strName}/{str(uuid.uuid4())}.parquet')
    blob.upload_from_filename(filename)
