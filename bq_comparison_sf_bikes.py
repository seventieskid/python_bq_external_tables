from google.cloud import bigquery
import os
import time
from tabulate import tabulate

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/garethrees/Downloads/k8s-play-unique-e5b5cf98bbc7.json"

# BQ public source data: bigquery-public-data.san_francisco_bikeshare.bikeshare_trips (983,648 rows only)

client = bigquery.Client()

native_query = """
    SELECT * FROM `DATAHUB.san_francisco_bikeshare_trips`
    WHERE bike_number = 11 AND end_station_id = 14
"""
external_query = """
    SELECT * FROM `DATAHUB_EXTERNAL.san_francisco_bikeshare_trips`
    WHERE bike_number = 11 AND end_station_id = 14
"""
external_hps_query = """
    SELECT * FROM `DATAHUB_EXTERNAL.san_francisco_bikeshare_trips_hps`
    WHERE hps_bike_number = 11 AND hps_end_station_id = 14
"""

job_config = bigquery.QueryJobConfig(use_query_cache=False)

#create data
table = []

col_names = ["Native SELECT (ms)", "External SELECT (ms)", "External With HPS SELECT (ms)", "External % Slower", "External With HPS % Slower"]

for i in range(200):
    row = [0,0,0,0,0]
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

    print(f'{i} - NATIVE={native_query_job.result().total_rows}, EXTERNAL={external_query_job.result().total_rows}, EXTERNAL_HPS={external_with_hps_query_job.result().total_rows}')

    row[3] = round(((row[1] - row[0])/(row[0])) * 100, 0)

    row[4] = round(((row[2] - row[0])/(row[0])) * 100, 0)

    table.append(row)

print(tabulate(table, headers=col_names, tablefmt="grid", showindex="always"))

content2=tabulate(table, tablefmt="tsv")
text_file=open("200_iters_select_bike_number_11_end_station_14_983648_standard.csv","w")
text_file.write(content2)
text_file.close()
