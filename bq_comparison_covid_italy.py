from google.cloud import bigquery
import os
import time
from tabulate import tabulate

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/garethrees/Downloads/k8s-play-unique-e5b5cf98bbc7.json"

# BQ public source data: bigquery-public-data.covid19_italy.national_trends (1291 rows only)

client = bigquery.Client()

native_query = """
    SELECT * FROM `DATAHUB.covid19_italy_national_trends`
    WHERE date BETWEEN '2022-08-01 00:00:00' AND '2022-08-31 23:59:59'
"""
external_query = """
    SELECT * FROM `DATAHUB_EXTERNAL.covid19_italy_national_trends`
    WHERE date BETWEEN '2022-08-01 00:00:00' AND '2022-08-31 23:59:59'
"""
external_hps_query = """
    SELECT * FROM `DATAHUB_EXTERNAL.covid19_italy_national_trends_with_hps`
    WHERE   hps_date = '2022-08-01' OR
            hps_date = '2022-08-02' OR
            hps_date = '2022-08-03' OR
            hps_date = '2022-08-04' OR
            hps_date = '2022-08-05' OR 
            hps_date = '2022-08-06' OR
            hps_date = '2022-08-07' OR
            hps_date = '2022-08-08' OR
            hps_date = '2022-08-09' OR
            hps_date = '2022-08-10' OR
            hps_date = '2022-08-11' OR
            hps_date = '2022-08-12' OR
            hps_date = '2022-08-13' OR
            hps_date = '2022-08-14' OR
            hps_date = '2022-08-15' OR
            hps_date = '2022-08-16' OR
            hps_date = '2022-08-17' OR
            hps_date = '2022-08-18' OR
            hps_date = '2022-08-19' OR
            hps_date = '2022-08-20' OR
            hps_date = '2022-08-21' OR
            hps_date = '2022-08-22' OR
            hps_date = '2022-08-23' OR
            hps_date = '2022-08-24' OR
            hps_date = '2022-08-25' OR
            hps_date = '2022-08-26' OR
            hps_date = '2022-08-27' OR
            hps_date = '2022-08-28' OR
            hps_date = '2022-08-29' OR
            hps_date = '2022-08-30' OR
            hps_date = '2022-08-31'
"""

job_config = bigquery.QueryJobConfig(use_query_cache=False)

#create data
table = []

col_names = ["Native SELECT (ms)", "External SELECT (ms)", "External With HPS SELECT (ms)", "External % Slower", "External With HPS % Slower"]

for i in range(10):
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

# content2=tabulate(table, tablefmt="tsv")
# text_file=open("output_200_select_Aug22_1291_archive.csv","w")
# text_file.write(content2)
# text_file.close()