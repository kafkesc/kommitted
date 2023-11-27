use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use prometheus::{Registry, TextEncoder};
use tokio_util::sync::CancellationToken;

use crate::cluster_status::ClusterStatusRegister;
use crate::lag_register::LagRegister;
use crate::partition_offsets::PartitionOffsetsRegister;
use crate::prometheus_metrics::bespoke::*;

// TODO https://github.com/kafkesc/kommitted/issues/47
// TODO https://github.com/kafkesc/kommitted/issues/48
// TODO https://github.com/kafkesc/kommitted/issues/50
// TODO https://github.com/kafkesc/kommitted/issues/51
// TODO https://github.com/kafkesc/kommitted/issues/49

#[derive(Clone)]
struct HttpServiceState {
    cs_reg: Arc<ClusterStatusRegister>,
    po_reg: Arc<PartitionOffsetsRegister>,
    lag_reg: Arc<LagRegister>,
    metrics: Arc<Registry>,
}

pub async fn init(
    listen_on: SocketAddr,
    cs_reg: Arc<ClusterStatusRegister>,
    po_reg: Arc<PartitionOffsetsRegister>,
    lag_reg: Arc<LagRegister>,
    shutdown_token: CancellationToken,
    metrics: Arc<Registry>,
) {
    // Assemble the HTTP Service State object, that will be passed to the routes
    let state = HttpServiceState {
        cs_reg,
        po_reg,
        lag_reg,
        metrics,
    };

    // Setup Router
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/", get(root))
        .route("/metrics", get(prometheus_metrics))
        .with_state(state);

    // Setup Server, with Graceful Shutdown
    let server = axum::Server::bind(&listen_on)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_token.cancelled());

    info!("Begin listening on '{}'...", listen_on);
    server
        .await
        .expect("HTTP Graceful Shutdown handler returned an error - this should never happen");
}

async fn root() -> &'static str {
    "Hello, World!"
}

async fn prometheus_metrics(State(state): State<HttpServiceState>) -> impl IntoResponse {
    let mut status = StatusCode::OK;
    let mut headers = HeaderMap::new();

    // Procure the Cluster ID once and reuse it in all metrics that get generated
    let cluster_id = state.cs_reg.get_cluster_id().await;

    // Procure the TopicPartitions once and reuse it in all metrics that need it
    let tps = state.cs_reg.get_topic_partitions().await;

    // As defined by Prometheus: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md#basic-info
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; version=0.0.4"));

    // Allocate a Vector of Strings to build the body of the output.
    // The capacity is pre-calculated to try to do as little mem-alloc as possible.
    //
    // The capacity is necessarily a function of the number of metric types produced,
    // and the number of topic partitions.
    let tp_count: usize = state
        .lag_reg
        .lag_by_group
        .read()
        .await
        .iter()
        .map(|(_, gwl)| gwl.lag_by_topic_partition.len())
        .sum();
    let metric_types_count: usize = 3;
    let headers_footers_count: usize = metric_types_count * 2;
    let metrics_count: usize = tp_count * metric_types_count;
    let mut body: Vec<String> = Vec::with_capacity(metrics_count + headers_footers_count);

    // ----------------------------------------------------------- METRIC: consumer_partition_offset
    consumer_partition_offset::append_headers(&mut body);
    iter_lag_reg(&state.lag_reg, &mut body, &cluster_id, consumer_partition_offset::append_metric)
        .await;

    // ------------------------------------------------------- METRIC: consumer_partition_lag_offset
    consumer_partition_lag_offset::append_headers(&mut body);
    iter_lag_reg(
        &state.lag_reg,
        &mut body,
        &cluster_id,
        consumer_partition_lag_offset::append_metric,
    )
    .await;

    // ------------------------------------------------- METRIC: consumer_partition_lag_milliseconds
    consumer_partition_lag_milliseconds::append_headers(&mut body);
    iter_lag_reg(
        &state.lag_reg,
        &mut body,
        &cluster_id,
        consumer_partition_lag_milliseconds::append_metric,
    )
    .await;

    // ------------------------------------------------- METRIC: partition_earliest_available_offset
    partition_earliest_available_offset::append_headers(&mut body);
    for tp in tps.iter() {
        match state.po_reg.get_earliest_available_offset(tp).await {
            Ok(eao) => {
                partition_earliest_available_offset::append_metric(
                    &cluster_id,
                    &tp.topic,
                    tp.partition,
                    eao,
                    &mut body,
                );
            },
            Err(e) => {
                warn!("Unable to generate 'partition_earliest_available_offset': {e}");
            },
        }
    }

    // ------------------------------------------------- METRIC: partition_latest_available_offset
    partition_latest_available_offset::append_headers(&mut body);
    for tp in tps.iter() {
        match state.po_reg.get_latest_available_offset(tp).await {
            Ok(lao) => {
                partition_latest_available_offset::append_metric(
                    &cluster_id,
                    &tp.topic,
                    tp.partition,
                    lao,
                    &mut body,
                );
            },
            Err(e) => {
                warn!("Unable to generate 'partition_latest_available_offset': {e}");
            },
        }
    }

    // ------------------------------------------------- METRIC: partition_earliest_tracked_offset
    partition_earliest_tracked_offset::append_headers(&mut body);
    for tp in tps.iter() {
        match state.po_reg.get_earliest_tracked_offset(tp).await {
            Ok(eto) => {
                partition_earliest_tracked_offset::append_metric(
                    &cluster_id,
                    &tp.topic,
                    tp.partition,
                    eto.offset,
                    eto.at.timestamp_millis(),
                    &mut body,
                );
            },
            Err(e) => {
                warn!("Unable to generate 'partition_earliest_tracked_offset': {e}");
            },
        }
    }

    // ------------------------------------------------- METRIC: partition_latest_tracked_offset
    partition_latest_tracked_offset::append_headers(&mut body);
    for tp in tps.iter() {
        match state.po_reg.get_latest_tracked_offset(tp).await {
            Ok(lto) => {
                partition_latest_tracked_offset::append_metric(
                    &cluster_id,
                    &tp.topic,
                    tp.partition,
                    lto.offset,
                    lto.at.timestamp_millis(),
                    &mut body,
                );
            },
            Err(e) => {
                warn!("Unable to generate 'partition_latest_tracked_offset': {e}");
            },
        }
    }

    // --- CLUSTER METRICS ---
    //
    // TODO https://github.com/kafkesc/kommitted/issues/54

    // --- KOMMITTED INTERNAL METRICS ---
    //
    // TODO https://github.com/kafkesc/kommitted/issues/56
    // TODO https://github.com/kafkesc/kommitted/issues/57

    // Turn the bespoke metrics created so far, into a String
    let mut body = body.join("\n");

    // Append to the bespoke metrics, classic Prometheus Metrics
    let metrics_family = state.metrics.gather();
    if let Err(e) = TextEncoder.encode_utf8(&metrics_family, &mut body) {
        status = StatusCode::INTERNAL_SERVER_ERROR;
        body = format!("Failed to encode metrics: {e}");
    }

    (status, headers, body)
}
