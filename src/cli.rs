use clap::{ArgGroup, Parser};
use rdkafka::ClientConfig;

// Follows the list of arguments we need to start with:
//
// TODO doc
//
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
    #[arg(long = "client-id", value_name = "CLIENT_ID", default_value = env ! ("CARGO_PKG_NAME"))]
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

    /// For each Topic Partition, how much history of offsets to keep in memory.
    ///
    /// Offsets data points are collected _at best every second_.
    /// Once this limit is reached, the oldest data points are discarded, realising
    /// a "moving window" of offsets history.
    #[arg(
        short = 'h',
        long = "history",
        value_name = "SIZE",
        default_value = "3600",
        verbatim_doc_comment
    )]
    pub offsets_history: usize,

    /// Verbose logging.
    ///
    /// * none    = 'WARN'
    /// * '-v'    = 'INFO'
    /// * '-vv'   = 'DEBUG'
    /// * '-vvv'  = 'TRACE'
    ///
    /// Alternatively, set environment variable 'KCL_LOG=(ERROR|WARN|INFO|DEBUG|TRACE|OFF)'.
    #[arg(short, long, action = clap::ArgAction::Count, verbatim_doc_comment)]
    pub verbose: u8,

    /// Quiet logging.
    ///
    /// * none    = 'WARN'
    /// * '-q'    = 'ERROR'
    /// * '-qq'   = 'OFF'
    ///
    /// Alternatively, set environment variable 'KCL_LOG=(ERROR|WARN|INFO|DEBUG|TRACE|OFF)'.
    #[arg(short, long, action = clap::ArgAction::Count, verbatim_doc_comment)]
    pub quiet: u8,
}

impl Cli {
    pub fn parse_and_validate() -> Self {
        // TODO Implement a proper validation
        Self::parse()
    }

    pub fn verbosity_level(&self) -> i8 {
        self.verbose as i8 - self.quiet as i8
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
