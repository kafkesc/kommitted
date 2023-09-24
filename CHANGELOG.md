# vNEXT (2023-??-??)

## Notes

* Introduced (this) [`CHANGELOG.md`](./CHANGELOG.md) and accompanying [`CHANGELOG_GUIDANCE.md`](./CHANGELOG_GUIDANCE.md) ([I#52](https://github.com/kafkesc/kommitted/issues/52))
* Added build badges to [`README.md`](./README.md) ([I#62](https://github.com/kafkesc/kommitted/issues/62))

# v0.2.0 (2023-09-24)

## Enhancements

* Refined logging to avoid polluting logs with temporary startup-time issues (i.e. not enough offset or lag info yet to produce estimations)
* `partition_offsets` module: Increased frequency of fetch of offset watermark
* New CLI argument `--history-ready-at`

## Breaking changes

* Separated the arguments to define `--host` and `--port` to listen on

## Notes

* Multiple dependencies upgrade

# v0.1.x (2023-08)

## Features

* First fully working release
* Production of multiple lag metrics:
  * Consumer Metrics
    * `kmtd_kafka_consumer_partition_lag_milliseconds`
    * `kmtd_kafka_consumer_partition_lag_offset`
    * `kmtd_kafka_consumer_partition_offset`
  * Topic Partition Metrics
    * `kmtd_kafka_partition_earliest_available_offset`
    * `kmtd_kafka_partition_latest_available_offset`
  * Topic Partition Offset Tracking Metrics
    * `kmtd_kafka_partition_earliest_tracked_offset`
    * `kmtd_kafka_partition_latest_tracked_offset`

## Enhancements

* Documented all exported metrics in [`METRICS.md`](./METRICS.md)
* Setup CI automation and automatic publishing when commit `vX.Y.Z` is pushed