use std::net::{IpAddr, SocketAddr};

use clap::{ArgGroup, Parser};
use rdkafka::ClientConfig;

use crate::constants::{
    DEFAULT_HTTP_HOST, DEFAULT_HTTP_PORT, DEFAULT_OFFSETS_HISTORY, DEFAULT_OFFSETS_HISTORY_READY_AT,
};

/// Command Line Interface, defined via the declarative,
/// `derive` based functionality of the `clap` crate.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(group(
    ArgGroup::new("logging_flags")
        .required(false)
        .multiple(false)
        .args(["verbose", "quiet"]),
))]
pub struct Cli {
    // ------------------------------------------------------------------ Admin Client configuration
    /// Initial Kafka Brokers to connect to (format: 'HOST:PORT,...').
    ///
    /// Equivalent to '--config=bootstrap.servers:host:port,...'.
    #[arg(short, long = "brokers", value_name = "BOOTSTRAP_BROKERS")]
    pub bootstrap_brokers: String,

    /// Client identifier used by the internal Kafka (Admin) Client.
    ///
    /// Equivalent to '--config=client.id:my-client-id'.
    #[arg(long = "client-id", value_name = "CLIENT_ID", default_value = env!("CARGO_PKG_NAME"))]
    pub client_id: String,

    /// Additional configuration used by the internal Kafka (Admin) Client (format: 'CONF_KEY:CONF_VAL').
    ///
    /// To set multiple configurations keys, use this argument multiple times.
    /// See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    #[arg(
        long = "kafka-conf",
        value_name = "CONF_KEY:CONF_VAL",
        value_parser = kv_clap_value_parser,
        verbatim_doc_comment
    )]
    pub kafka_config: Vec<KVPair>,

    /// Override identifier of the monitored Kafka Cluster.
    ///
    /// If set, it replaces the value `cluster.id` from the Brokers' configuration.
    /// This can be useful when `cluster.id` is not actually set.
    #[arg(long = "cluster-id", value_name = "CLUSTER_ID")]
    pub cluster_id: Option<String>,

    /// For each Topic Partition, how much history of offsets to track in memory.
    ///
    /// Offsets data points are collected every 500ms, on average: so, on average,
    /// 30 minutes of data points is 3600 offsets, assuming partition offsets are
    /// regularly produced to.
    ///
    /// Once this limit is reached, the oldest data points are discarded, realising
    /// a "moving window" of offsets history.
    #[arg(
        long = "history",
        value_name = "SIZE_PER_PARTITION",
        default_value = DEFAULT_OFFSETS_HISTORY,
        verbatim_doc_comment
    )]
    pub offsets_history: usize,

    /// How full `--history` of Topic Partition offsets has to be (on average) for service to be ready.
    ///
    /// This value will be compared with the average "fullness" of each data structure containing
    /// the offsets of Topic Partitions. Once passed, the service can start serving metrics.
    ///
    /// The value must be a percentage in the range `[0.0%, 100.0%]`.
    #[arg(
        long = "history-ready-at",
        value_name = "FULLNESS_PERCENT_PER_PARTITION",
        default_value = DEFAULT_OFFSETS_HISTORY_READY_AT,
        value_parser = percent_clap_value_parser,
        verbatim_doc_comment
    )]
    pub offsets_history_ready_at: f64,

    /// Host address to listen on for HTTP requests.
    ///
    /// Supports both IPv4 and IPv6 addresses.
    #[arg(long, default_value = DEFAULT_HTTP_HOST, verbatim_doc_comment)]
    pub host: IpAddr,

    /// Port to listen on for HTTP requests.
    #[arg(long, default_value = DEFAULT_HTTP_PORT, verbatim_doc_comment)]
    pub port: u16,

    /// Verbose logging.
    ///
    /// * none    = 'WARN'
    /// * '-v'    = 'INFO'
    /// * '-vv'   = 'DEBUG'
    /// * '-vvv'  = 'TRACE'
    ///
    /// Alternatively, set environment variable 'KOMMITTED_LOG=(ERROR|WARN|INFO|DEBUG|TRACE|OFF)'.
    #[arg(short, long, action = clap::ArgAction::Count, verbatim_doc_comment)]
    pub verbose: u8,

    /// Quiet logging.
    ///
    /// * none    = 'WARN'
    /// * '-q'    = 'ERROR'
    /// * '-qq'   = 'OFF'
    ///
    /// Alternatively, set environment variable 'KOMMITTED_LOG=(ERROR|WARN|INFO|DEBUG|TRACE|OFF)'.
    #[arg(short, long, action = clap::ArgAction::Count, verbatim_doc_comment)]
    pub quiet: u8,
}

impl Cli {
    pub fn verbosity_level(&self) -> i8 {
        self.verbose as i8 - self.quiet as i8
    }

    pub fn listen_on(&self) -> SocketAddr {
        SocketAddr::from((self.host, self.port))
    }

    pub fn build_client_config(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", self.bootstrap_brokers.clone())
            .set("client.id", self.client_id.clone());
        for cfg in &self.kafka_config {
            config.set(cfg.0.clone(), cfg.1.clone());
        }

        trace!("Created:\n{:#?}", config);
        config
    }
}

/// A simple (key,value) pair of `String`s, useful to be parsed from arguments via [`kv_clap_value_parser`].
pub type KVPair = (String, String);

/// To be used as [`clap::value_parser`] function to create [`KVPair`] values.
fn kv_clap_value_parser(kv: &str) -> Result<KVPair, String> {
    let (k, v) = match kv.split_once(':') {
        None => {
            return Err("Should have 'K:V' format".to_string());
        },
        Some((k, v)) => (k, v),
    };

    Ok((k.to_string(), v.to_string()))
}

fn percent_clap_value_parser(percent_str: &str) -> Result<f64, String> {
    let percent =
        percent_str.parse::<f64>().map_err(|e| format!("Unable to parse {percent_str}: {e}"))?;

    if !(0.0..=100.0).contains(&percent) {
        return Err(format!("Percentage value {percent} should be between [0.0%, 100.0%]"));
    }

    Ok(percent)
}
