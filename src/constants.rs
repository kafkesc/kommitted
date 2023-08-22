use const_format::formatcp;

/// Kafka internal topic that keeps track of Consumer's committed Offsets.
/// This is consumed inside the `konsumer_offsets_data` module.
pub(crate) const KONSUMER_OFFSETS_DATA_TOPIC: &str = "__consumer_offsets";

/// This is the Consumer Group (`group.id`) value used by
/// the Consumer inside the `konsumer_offsets_data` module.
pub(crate) const KOMMITTED_CONSUMER_OFFSETS_CONSUMER: &str =
    "__kommitted__consumer_offsets_consumer";

/// The default host to bind to when launching internal HTTP server.
pub(crate) const DEFAULT_HTTP_HOST: &str = "localhost";

/// The default port to bind to when launching internal HTTP server.
pub(crate) const DEFAULT_HTTP_PORT: &str = "9090";

/// The default host:port to bind to when launching internal HTTP server.
pub(crate) const DEFAULT_HTTP_HOST_PORT: &str =
    formatcp!("{DEFAULT_HTTP_HOST}:{DEFAULT_HTTP_PORT}");
