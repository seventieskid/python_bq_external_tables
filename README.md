# python_bq_external_tables

## San Francisco (983,648 rows)
Dataset: bigquery-public-data.san_francisco.bikeshare_trips

```
CREATE OR REPLACE EXTERNAL TABLE
  `DATAHUB_EXTERNAL.san_francisco_bikeshare_trips_biglake`
  WITH CONNECTION `358013237822.europe-west2.san_francisco_bikeshare_trips`
  OPTIONS(
    format = "PARQUET",
    uris = ['gs://k8s-play-unique-fotc-cfs/san_francisco_bikeshare_trips/data.parquet.gzip'],
    max_staleness = INTERVAL 4 HOUR,
    metadata_cache_mode = 'AUTOMATIC'
    );

CREATE OR REPLACE EXTERNAL TABLE
  `DATAHUB_EXTERNAL.san_francisco_bikeshare_trips_biglake_hps`
  WITH PARTITION COLUMNS
  (
    hps_bike_number INTEGER,
    hps_end_station_id INTEGER
  )
  WITH CONNECTION `358013237822.europe-west2.san_francisco_bikeshare_trips_hps`
  OPTIONS(
    format = "PARQUET",
    hive_partition_uri_prefix = "gs://k8s-play-unique-fotc-cfs/san_francisco_bikeshare_trips_hps",
    uris = ['gs://k8s-play-unique-fotc-cfs/san_francisco_bikeshare_trips_hps/*'],
    max_staleness = INTERVAL 4 HOUR,
    metadata_cache_mode = 'AUTOMATIC'
    );
```
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
    hive_partition_uri_prefix = "gs://k8s-play-unique-new-york-citibike/new_york_bikeshare_trips_hps",
    uris = ['gs://k8s-play-unique-new-york-citibike/new_york_bikeshare_trips_hps/*'],
    max_staleness = INTERVAL 4 HOUR,
    metadata_cache_mode = 'AUTOMATIC'
    );
```