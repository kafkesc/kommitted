# vNEXT

## Notes

* Updated [`axum`](https://crates.io/crates/axum) (a key dependency) to `v0.7` ([PR#113](https://github.com/kafkesc/kommitted/pull/113))
* Removed dependency on [`async-trait`](https://crates.io/crates/async-trait) (as per [Rust 1.75](https://blog.rust-lang.org/2023/12/21/async-fn-rpit-in-traits.html#where-the-gaps-lie)) ([PR#115](https://github.com/kafkesc/kommitted/pull/115))
* Deps upgrade
* `Dockerfile` base image updated
  to [`rust:1.77.2-slim-bookworm`](https://hub.docker.com/layers/library/rust/1.77.2-slim-bookworm/images/sha256-ed00d3908ba2ade42982456c2f9b8b6db5fecf14e9d1d2f58a1dedbb6a7c924e?context=explore)

# v0.2.2 (2023-11-19)

## Features

* Exposed `consumer_groups` module metrics ([I#53](https://github.com/kafkesc/kommitted/issues/53), [I#55](https://github.com/kafkesc/kommitted/issues/55))
* Exposed `partition_offsets` module metrics ([I#57](https://github.com/kafkesc/kommitted/issues/57))
* Exposed `cluster_status` module metrics ([I#54](https://github.com/kafkesc/kommitted/issues/54), [I#56](https://github.com/kafkesc/kommitted/issues/56))

## Enhancements

* Reworked workflows that publish Docker image: triggering 1 dedicated workflow per target ([I#80](https://github.com/kafkesc/kommitted/issues/80))

## Notes

* Multiple rounds of dependencies upgrade
* Updated [`METRICS.md`](./METRICS.md) with documentation for each of the added metrics

# v0.2.1 (2023-10-04)

## Features

* Setup GitHub Actions Workflow to publish Docker image [`kafkesc/kommitted`](https://hub.docker.com/r/kafkesc/kommitted) at every release ([I#64](https://github.com/kafkesc/kommitted/issues/64)) 

## Notes

* Introduced (this) [`CHANGELOG.md`](./CHANGELOG.md) and accompanying [`CHANGELOG_GUIDANCE.md`](./CHANGELOG_GUIDANCE.md) ([I#52](https://github.com/kafkesc/kommitted/issues/52))
* Added build badges to [`README.md`](./README.md) ([I#62](https://github.com/kafkesc/kommitted/issues/62))
* Multiple rounds of dependencies upgrade

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