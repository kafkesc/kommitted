# Design

<div align="center" style="text-align: center;">

**WORK IN PROGRESS**

</div>

## Architecture diagram (overview)

![](https://docs.google.com/drawings/d/e/2PACX-1vTJf5vkITRpDPlL-icLwYHRbUB7Y2KGbkkdcKNhECJ3tdrUJud9Cr3Hnowp_nLN55aiZuw01hmzXNmw/pub?w=1008&h=761)

## Key dependencies

* [konsumer_offsets](https://crates.io/crates/konsumer_offsets):
  Most complete parser of records in `__consumer_offsets`
* [rdkafka](https://crates.io/crates/rdkafka):
  Rust-wrapper of [librdkafka](https://github.com/edenhill/librdkafka),
  best Kafka client library   
* [tokio](https://tokio.rs): Production-ready asynchronous runtime for Rust
* [clap](https://crates.io/crates/clap): Most complete Command Line Argument Parser