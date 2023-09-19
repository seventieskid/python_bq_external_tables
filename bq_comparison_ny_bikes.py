from google.cloud import bigquery
import os
import time
from tabulate import tabulate

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/garethrees/Downloads/k8s-play-unique-e5b5cf98bbc7.json"

# BQ public source data: bigquery-public-data.new_york_citibike.citibike_trips (10,939,860rows only)

client = bigquery.Client()

native_query = """
    SELECT * FROM `DATAHUB.new_york_bikeshare_trips`
    WHERE bikeid = 14627 AND end_station_id = 72
"""
external_query = """
    SELECT * FROM `DATAHUB_EXTERNAL.new_york_bikeshare_trips`
    WHERE bikeid = 14627 AND end_station_id = 72
"""
external_hps_query = """
    SELECT * FROM `DATAHUB_EXTERNAL.new_york_bikeshare_trips_hps`
    WHERE hps_bike_number = 14627 AND hps_end_station_id = 72
"""
external_biglake_query = """
    SELECT * FROM `DATAHUB_EXTERNAL.new_york_bikeshare_trips_biglake`
    WHERE bikeid = 14627 AND end_station_id = 72
"""
external_biglake_hps_query = """
    SELECT * FROM `DATAHUB_EXTERNAL.new_york_bikeshare_trips_biglake_hps`
    WHERE hps_bike_number = 14627 AND hps_end_station_id = 72
"""

job_config = bigquery.QueryJobConfig(use_query_cache=False)

#create data
table = []

col_names = [ "Native SELECT (secs)", \
             "External SELECT (secs)", \
             "External With HPS SELECT (secs)", \
             "External Big Lake SELECT (secs)", \
             "External Big Lake With HPS SELECT (secs)", \
             "External % Slower", \
             "External With HPS % Slower", \
             "External Big Lake % Slower", \
             "External Big Lake With HPS % Slower" ]

# Flags used to generate unique output file names
samples = 1000
row_count = 1009431
metadata_caching = "on"

for i in range(samples):
    row = [0,0,0,0,0,0,0,0,0]
    start_time = time.time()
    native_query_job = client.query(native_query, job_config)
    row[0] = time.time() - start_time

    start_time = time.time()
    clientExternal = bigquery.Client()
    external_query_job = clientExternal.query(external_query, job_config)
    row[1] = time.time() - start_time

    start_time = time.time()
    clientExternalWithHPS = bigquery.Client()
    external_with_hps_query_job = clientExternalWithHPS.query(external_hps_query, job_config)
    row[2] = time.time() - start_time

    start_time = time.time()
    clientExternalBigLake = bigquery.Client()
    external_biglake_query_job = clientExternalBigLake.query(external_biglake_query, job_config)
    row[3] = time.time() - start_time

    start_time = time.time()
    clientExternalBigLakeWithHPS = bigquery.Client()
    external_biglake_hps_query_job = clientExternalBigLakeWithHPS.query(external_biglake_hps_query, job_config)
    row[4] = time.time() - start_time

    print(f'{i} - NATIVE={native_query_job.result().total_rows}, \
EXTERNAL={external_query_job.result().total_rows}, \
EXTERNAL_HPS={external_with_hps_query_job.result().total_rows} \
EXTERNAL_BIGLAKE={external_biglake_query_job.result().total_rows}, \
EXTERNAL_BIGLAKE_HPS={external_biglake_hps_query_job.result().total_rows} ')

#     print(f'{i} - NATIVE={native_query_job.result().total_rows}, \
# EXTERNAL_BIGLAKE={external_biglake_query_job.result().total_rows}, \
# EXTERNAL_BIGLAKE_HPS={external_biglake_hps_query_job.result().total_rows} ')
    
    row[5] = round(((row[1] - row[0])/(row[0])) * 100, 0)

    row[6] = round(((row[2] - row[0])/(row[0])) * 100, 0)

    row[7] = round(((row[3] - row[0])/(row[0])) * 100, 0)

    row[8] = round(((row[4] - row[0])/(row[0])) * 100, 0)

    table.append(row)

with open(f'results/new_york/{samples}_iters_select_bikeid_14627_end_station_72_{row_count}_standard_biglake_metadata_caching_{metadata_caching}.csv', 'w') as f:
    f.write(tabulate(table, headers=col_names))
