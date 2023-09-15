from google.cloud import bigquery
from google.cloud import storage
import os
import pandas as pd
import pandas_gbq
import uuid

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/garethrees/Downloads/k8s-play-unique-e5b5cf98bbc7.json"

BUCKET_NAME = "k8s-play-unique-fotc-cfs"

sql = """
    SELECT * FROM `k8s-play-unique.DATAHUB.san_francisco_bikeshare_trips`
"""

configuration = {
   'query': {
     "useQueryCache": False
   }
}

df = pandas_gbq.read_gbq(sql, configuration=configuration)
df_bike_number_groups = df.groupby(df['bike_number'])

client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)

print(f'Bike number: {df_bike_number_groups.ngroups}')

partitions = 0

for bike_number, df_bike_number_group in df_bike_number_groups:

    df_end_station_id_groups = df_bike_number_group.groupby(df['end_station_id'])

    print(f'\tEnd station id: {df_end_station_id_groups.ngroups}')

    for end_station_id, df_end_station_id_group in df_end_station_id_groups:
      # Create a filename based on the group name
      filename = f'{bike_number}_{end_station_id}.parquet'
      
      # Write the group to a CSV file
      df_end_station_id_group.to_parquet(filename, compression='gzip', index=False)

      blob = bucket.blob(f'san_francisco_bikeshare_trips_hps/hps_bike_number={bike_number}/hps_end_station_id={end_station_id}/{str(uuid.uuid4())}.parquet')
      blob.upload_from_filename(filename)

      # Remove local file
      os.remove(filename)

      partitions = partitions + 1

print(f'Total partitions={partitions}')