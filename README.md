# python_bq_external_tables

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
    hive_partition_uri_prefix = "gs://k8s-play-unique-fotc-cfs/san_francisco_bikeshare_trips_biglake_hps",
    uris = ['gs://k8s-play-unique-fotc-cfs/san_francisco_bikeshare_trips_biglake_hps/*'],
    max_staleness = INTERVAL 4 HOUR,
    metadata_cache_mode = 'AUTOMATIC'
    );