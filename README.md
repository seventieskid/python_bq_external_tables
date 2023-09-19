# python_bq_external_tables

## New York (10,939,860 rows)
Dataset: bigquery-public-data.new_york_citibike.citibike_trips

DELETE FROM DATAHUB.new_york_citibike_bikeshare_trips where tripduration is null

DELETE FROM DATAHUB.new_york_citibike_bikeshare_trips where bikeid >= 16806

```
CREATE OR REPLACE EXTERNAL TABLE
  `DATAHUB_EXTERNAL.new_york_bikeshare_trips_biglake`
  WITH CONNECTION `358013237822.europe-west2.new_york_bikeshare_trips_biglake`
  OPTIONS(
    format = "PARQUET",
    uris = ['gs://k8s-play-unique-new-york-citibike/raw/*'],
    max_staleness = INTERVAL 4 HOUR,
    metadata_cache_mode = 'AUTOMATIC'
    );

CREATE OR REPLACE EXTERNAL TABLE
  `DATAHUB_EXTERNAL.new_york_bikeshare_trips_biglake_hps`
  WITH PARTITION COLUMNS
  (
    hps_bike_number INTEGER,
    hps_end_station_id INTEGER
  )
  WITH CONNECTION `358013237822.europe-west2.new_york_bikeshare_trips_biglake_hps`
  OPTIONS(
    format = "PARQUET",
    hive_partition_uri_prefix = "gs://k8s-play-unique-new-york-citibike/new_york_bikeshare_trips_biglake_hps",
    uris = ['gs://k8s-play-unique-new-york-citibike/new_york_bikeshare_trips_biglake_hps/*'],
    max_staleness = INTERVAL 4 HOUR,
    metadata_cache_mode = 'AUTOMATIC'
    );
```