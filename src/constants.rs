/// Kafka internal topic that keeps track of Consumer's committed Offsets.
/// This is consumed inside the `konsumer_offsets_data` module.
pub(crate) const KONSUMER_OFFSETS_DATA_TOPIC: &str = "__consumer_offsets";

/// This is the Consumer Group (`group.id`) value used by
/// the Consumer inside the `konsumer_offsets_data` module.
pub(crate) const KOMMITTED_CONSUMER_OFFSETS_CONSUMER: &str =
    "__kommitted__consumer_offsets_consumer";

/// The default host to listen on when launching the HTTP server.
pub(crate) const DEFAULT_HTTP_HOST: &str = "127.0.0.1";

/// The default port to listen on when launching the HTTP server.
///
/// Why `6564`? `hex("kommitted") = 6b6f6d6d6974746564`, and I picked the last 4 digits.
pub(crate) const DEFAULT_HTTP_PORT: &str = "6564"; //< `u16` after parsing

/// The default amount of offsets history to track in memory.
///
/// See [`crate::Cli`]'s `offsets_history`.
pub(crate) const DEFAULT_OFFSETS_HISTORY: &str = "3600"; //< `usize` after parsing

/// The default fullness percentage" at which the Partition Offset Register can be considered ready.
///
/// See [`crate::Cli`]'s `offsets_history_ready_at`.
pub(crate) const DEFAULT_OFFSETS_HISTORY_READY_AT: &str = "0.3"; //< `f64` after parsing
