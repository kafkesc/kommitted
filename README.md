# Kommitted - Measure Kafka consumers lag

<div align="center" style="text-align: center;">

Measure Kafka Consumer **Offset Lag** _and_ **Time Lag**

[![CI](https://img.shields.io/github/actions/workflow/status/kafkesc/kommitted/ci.yml?branch=main&label=CI%20%28main%29&logo=Github&style=flat-square)](https://github.com/kafkesc/kommitted/actions/workflows/ci.yml)
[![Apache 2.0](https://img.shields.io/crates/l/kommitted?logo=apache&style=flat-square)](https://github.com/search?q=repo%3Akafkesc%2Fkommitted+path%3ALICENSE&type=code)
[![Crates.io downloads](https://img.shields.io/crates/d/kommitted?logo=rust&style=flat-square)](https://crates.io/crates/kommitted)
[![](https://img.shields.io/crates/v/kommitted?label=latest%20version&logo=rust&style=flat-square)](https://crates.io/crates/kommitted/versions)

</div>

**Kommitted** is a service to measure the _Lag_ (i.e. _Latency_) of Kafka consumers.
It works with all consumers that _commit_ their offsets into Kafka
(i.e. the [standard way](https://kafka.apache.org/documentation/#design_consumerposition)),
as it consumes the internal `__consumer_offsets` topic.

Metrics are exported following the [Prometheus](https://prometheus.io/)
[Exposition formats](https://prometheus.io/docs/instrumenting/exposition_formats/#exposition-formats).

Please see [DESIGN.md](./DESIGN.md) for details about the overall architecture, dependencies and other details.

## Features

* [x] Track Offset for all consumers
* [x] Track Offset Lag for all consumers
* [x] Track Time Lag for all consumers
* [x] Offset and Lag metrics are tracked with all contextual information to identify exact topic partition assignments
* [ ] Exposes additional metrics to track status of Kafka cluster (topics, members, brokers, partitions)
* [ ] Exposes Kafka-polling metrics, to assess its own performance
* [x] Metrics exposed in [Prometheus format](https://prometheus.io/docs/instrumenting/exposition_formats/#exposition-formats), at `/metrics` endpoint
* [ ] REST API to build further automation on top of it (e.g. auto-scaling logics that depend on Consumer Group lag)

All of this comes based on:

* A fast and efficient [Rust](https://rust-lang.org) implementation, built on [Tokio](https://tokio.rs/)
* The widely used [librdkafka](https://github.com/confluentinc/librdkafka/), the _de-facto_ standard for Kafka Clients (outside of Java)

Please see the complete [list of exposed Metrics](./METRICS.md), for further details.

## Getting started

To install `kommitted`, you need to compile it yourself, or use the [Docker image](#in-docker).
If you have the [Rust Toolchain](https://rustup.rs/) already setup, then just run:

```shell
$ cargo install kommitted
```

### In Docker

Kommitted is now available as a Docker Image: [`kafkesc/kommitted`](https://hub.docker.com/r/kafkesc/kommitted)
on the Docker Hub registry. Both `linux/amd64` and `linux/arm64` images are available, based on Debian slim images.

The `ENTRYPOINT` is the `kommitted` binary itself, so you can just pass arguments to the container execution.

## Usage

Kommitted supports _compact_ (`-h`) and _extended_ (`--help`) usage instructions.
Use the former for a quick look up; use the latter to better understand what
each argument can do.

<details open>
  <summary>Compact: `kommitted -h`</summary>

  ```shell
  Usage: kommitted [OPTIONS] --brokers <BOOTSTRAP_BROKERS>
  
  Options:
    -b, --brokers <BOOTSTRAP_BROKERS>
            Initial Kafka Brokers to connect to (format: 'HOST:PORT,...')
        --client-id <CLIENT_ID>
            Client identifier used by the internal Kafka (Admin) Client [default: kommitted]
        --kafka-conf <CONF_KEY:CONF_VAL>
            Additional configuration used by the internal Kafka (Admin) Client (format: 'CONF_KEY:CONF_VAL').
        --cluster-id <CLUSTER_ID>
            Override identifier of the monitored Kafka Cluster
        --history <SIZE_PER_PARTITION>
            For each Topic Partition, how much history of offsets to track in memory. [default: 3600]
        --history-ready-at <FULLNESS_PERCENT_PER_PARTITION>
            How full `--history` of Topic Partition offsets has to be (on average) for service to be ready. [default: 0.3]
        --host <HOST>
            Host address to listen on for HTTP requests. [default: 127.0.0.1]
        --port <PORT>
            Port to listen on for HTTP requests. [default: 6564]
    -v, --verbose...
            Verbose logging.
    -q, --quiet...
            Quiet logging.
    -h, --help
            Print help (see more with '--help')
    -V, --version
            Print version
  ```
</details>
  
<details>
  <summary>Extended: `kommitted --help`</summary>
  
  ```shell
  Usage: kommitted [OPTIONS] --brokers <BOOTSTRAP_BROKERS>
  
  Options:
    -b, --brokers <BOOTSTRAP_BROKERS>
            Initial Kafka Brokers to connect to (format: 'HOST:PORT,...').
  
            Equivalent to '--config=bootstrap.servers:host:port,...'.
  
        --client-id <CLIENT_ID>
            Client identifier used by the internal Kafka (Admin) Client.
  
            Equivalent to '--config=client.id:my-client-id'.
  
            [default: kommitted]
  
        --kafka-conf <CONF_KEY:CONF_VAL>
            Additional configuration used by the internal Kafka (Admin) Client (format: 'CONF_KEY:CONF_VAL').
  
            To set multiple configurations keys, use this argument multiple times.
            See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
  
        --cluster-id <CLUSTER_ID>
            Override identifier of the monitored Kafka Cluster.
  
            If set, it replaces the value `cluster.id` from the Brokers' configuration. This can be useful when `cluster.id` is not actually
            set.
  
        --history <SIZE_PER_PARTITION>
            For each Topic Partition, how much history of offsets to track in memory.
  
            Offsets data points are collected every 500ms, on average: so, on average,
            30 minutes of data points is 3600 offsets, assuming partition offsets are
            regularly produced to.
  
            Once this limit is reached, the oldest data points are discarded, realising
            a "moving window" of offsets history.
  
            [default: 3600]
  
        --history-ready-at <FULLNESS_PERCENT_PER_PARTITION>
            How full `--history` of Topic Partition offsets has to be (on average) for service to be ready.
  
            This value will be compared with the average "fullness" of each data structure containing
            the offsets of Topic Partitions. Once passed, the service can start serving metrics.
  
            The value must be a percentage in the range `[0.0%, 100.0%]`.
  
            [default: 0.3]
  
        --host <HOST>
            Host address to listen on for HTTP requests.
  
            Supports both IPv4 and IPv6 addresses.
  
            [default: 127.0.0.1]
  
        --port <PORT>
            Port to listen on for HTTP requests.
  
            [default: 6564]
  
    -v, --verbose...
            Verbose logging.
  
            * none    = 'WARN'
            * '-v'    = 'INFO'
            * '-vv'   = 'DEBUG'
            * '-vvv'  = 'TRACE'
  
            Alternatively, set environment variable 'KOMMITTED_LOG=(ERROR|WARN|INFO|DEBUG|TRACE|OFF)'.
  
    -q, --quiet...
            Quiet logging.
  
            * none    = 'WARN'
            * '-q'    = 'ERROR'
            * '-qq'   = 'OFF'
  
            Alternatively, set environment variable 'KOMMITTED_LOG=(ERROR|WARN|INFO|DEBUG|TRACE|OFF)'.
  
    -h, --help
            Print help (see a summary with '-h')
  
    -V, --version
            Print version
  ```
</details>

### Connect to Kafka cluster requiring [`SASL_SSL`](https://en.wikipedia.org/wiki/Simple_Authentication_and_Security_Layer)

```shell
$ kommitted \
    --brokers {{ BOOTSTRAP_BROKERS or BROKER_ENDPOINT }} \
    --config security.protocol:SASL_SSL \
    --config sasl.mechanisms=PLAIN \
    --config sasl.username:{{ USERNAME or API_KEY }} \
    --config sasl.password:{{ PASSWORD or API_SECRET }} \  
    ...
```

### Log verbosity

Kommitted follows the long tradition of `-v/-q` to control the verbosity of it's logging:

| Arguments | Log verbosity level | Default |
|----------:|:--------------------|:-------:|
|  `-qq...` | `OFF`               |         |
|      `-q` | `ERROR`             |         |
|    _none_ | `WARN`              |    x    |
|      `-v` | `INFO`              |         |
|     `-vv` | `DEBUG`             |         |
| `-vvv...` | `TRACE`             |         |

It uses [log](https://crates.io/crates/log) and [env_logger](https://crates.io/crates/env_logger),
and so logging can be configured and fine-tuned using the Environment Variable `KOMMITTED_LOG`.
Please take a look at [env_logger doc](https://docs.rs/env_logger/latest/env_logger/#enabling-logging)
for more details.

## License

Licensed under either of

* Apache License, Version 2.0
  ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license
  ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
