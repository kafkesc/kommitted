use std::str::FromStr;

use chrono::{FixedOffset, Local, Offset, TimeZone, Utc};
use chrono_tz::Tz;
use clap::{ArgGroup, Parser};
use rdkafka::ClientConfig;
use regex::{Captures, Regex, RegexSet};

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
    #[arg(short,
        long,
        value_name = "CONF_KEY:CONF_VAL",
        value_parser = kv_clap_value_parser,
        verbatim_doc_comment
    )]
    pub config: Vec<KVPair>,

    /// Timezone of the Kafka Brokers, compared to [UTC].
    ///
    /// This is used to interpret the committed offsets timestamps read from the `__consumer_offsets` topic.
    ///
    /// Defaults to the host system timezone, it supports the following format:
    ///
    /// * Timezone name compliant with [IANA Timezone DB] (e.g. "America/Los_Angeles")
    /// * Offset from UTC, in the format "±hh:mm:ss" (e.g. "+04:00:00")
    /// * Offset from UTC, in the format "±hh:mm" (e.g. "-08:00")
    ///
    /// [UTC]: https://en.wikipedia.org/wiki/Coordinated_Universal_Time
    /// [IANA Timezone DB]: https://www.iana.org/time-zones
    #[arg(long = "btz",
        value_name = "TIMEZONE",
        default_value_t = *(Local::now().offset()),
        value_parser = fixed_offset_clap_value_parser,
        allow_hyphen_values = true,
        verbatim_doc_comment
    )]
    pub brokers_timezone: FixedOffset,

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
        for cfg in &self.config {
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

fn fixed_offset_clap_value_parser(arg_tz: &str) -> Result<FixedOffset, String> {
    // Prepare set of supported format regexes
    let regex_set = RegexSet::new([
        r"^\w+/\w+$",
        r"^\w+$",
        r"^(?P<sign>[+\-])(?P<hours>\d{2}):(?P<minutes>\d{2}):(?P<seconds>\d{2})$",
        r"^(?P<sign>[+\-])(?P<hours>\d{2}):(?P<minutes>\d{2})$",
    ])
    .unwrap();
    // Compile each regex by itself as well, so they can be used for capturing
    let regexes: Vec<Regex> = regex_set
        .patterns()
        .iter()
        .map(|pattern| Regex::new(pattern).unwrap())
        .collect();

    // Check which regex matches
    let matches = regex_set.matches(arg_tz);

    return if matches.matched(0) || matches.matched(1) {
        // Case 0: IANA named Timezone

        Ok(arg_tz
            .parse::<Tz>()?
            .offset_from_utc_datetime(&Utc::now().naive_utc())
            .fix())
    } else if matches.matched(2) {
        // Case 1: Offset Timezone expressed as '±hh:mm:ss'

        let caps = regexes[2]
            .captures(arg_tz)
            .ok_or(format!("Failed to parse Timezone Offset: '{arg_tz}'"))?;

        let east = caps.name("sign").unwrap().as_str() == "+";
        let hours = cap_val::<i32>(&caps, "hours")?;
        let minutes = cap_val::<i32>(&caps, "minutes")?;
        let seconds = cap_val::<i32>(&caps, "seconds")?;

        let seconds = hours * 3600 + minutes * 60 + seconds;
        if east {
            Ok(FixedOffset::east_opt(seconds)
                .ok_or(format!("Out-of-bound Timezone Offset: '{arg_tz}'"))?)
        } else {
            Ok(FixedOffset::west_opt(seconds)
                .ok_or(format!("Out-of-bound Timezone Offset: '{arg_tz}'"))?)
        }
    } else if matches.matched(3) {
        // Case 2: Offset Timezone expressed as '±hh:mm'

        let caps = regexes[3]
            .captures(arg_tz)
            .ok_or(format!("Failed to parse Timezone Offset: '{arg_tz}'"))?;

        let east = caps.name("sign").unwrap().as_str() == "+";
        let hours = cap_val::<i32>(&caps, "hours")?;
        let minutes = cap_val::<i32>(&caps, "minutes")?;

        let seconds = hours * 3600 + minutes * 60;
        if east {
            Ok(FixedOffset::east_opt(seconds)
                .ok_or(format!("Out-of-bound Timezone Offset: '{arg_tz}'"))?)
        } else {
            Ok(FixedOffset::west_opt(seconds)
                .ok_or(format!("Out-of-bound Timezone Offset: '{arg_tz}'"))?)
        }
    } else {
        Err(format!(
            "'{arg_tz}' does not match format of IANA Timezone Name, nor of Timezone Offset (i.e. ±hh:mm / ±hh:mm:ss)"
        ))
    };
}

fn cap_val<T>(caps: &Captures, cap_group: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: ToString,
{
    caps.name(cap_group)
        .unwrap()
        .as_str()
        .parse::<T>()
        .map_err(|e| e.to_string())
}
