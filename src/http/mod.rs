mod metrics;

use std::{net::SocketAddr, sync::Arc};

use crate::cluster_status::ClusterStatusRegister;
use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use tokio_util::sync::CancellationToken;

use crate::lag_register::LagRegister;

// TODO HTTP Endpoints
//   /                Landing page
//   /metrics         Prometheus Metrics, filterable via `collect[]` or `name[]` array query param of metrics filter by
//   /status/healthy  Service healthy
//   /status/ready    Service ready (metrics are ready to be scraped)
//   /groups
//   /cluster
//
// TODO Add a layer of compression for GZip (optional for Prometheus)

#[derive(Clone)]
struct HttpServiceState {
    cs_reg: Arc<ClusterStatusRegister>,
    lag_reg: Arc<LagRegister>,
}

pub async fn init(cs_reg: Arc<ClusterStatusRegister>, lag_reg: Arc<LagRegister>, shutdown_token: CancellationToken) {
    let state = HttpServiceState {
        cs_reg,
        lag_reg,
    };

    // build our application with a route
    let app = Router::new()
        // `GET /` goes to `root`
        .route("/", get(root))
        .route("/metrics", get(prometheus_metrics))
        .with_state(state);

    // run our app with hyper
    // `axum::Server` is a re-export of `hyper::Server`
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    debug!("listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .with_graceful_shutdown(shutdown_token.cancelled())
        .await
        .expect("HTTP Graceful Shutdown handler returned an error - this should never happen")
}

async fn root() -> &'static str {
    "Hello, World!"
}

async fn prometheus_metrics(State(state): State<HttpServiceState>) -> impl IntoResponse {
    let status = StatusCode::OK;
    let mut headers = HeaderMap::new();

    // Procure the Cluster ID once and reuse it in all metrics that get generated
    let cluster_id = state.cs_reg.get_cluster_id().await;

    // As defined by Prometheus: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md#basic-info
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; version=0.0.4"));

    // Allocate a Vector of Strings to build the body of the output.
    // The capacity is pre-calculated to try to do as little mem-alloc as possible.
    //
    // The capacity is necessarily a function of the number of metric types produced,
    // and the number of topic partitions.
    let tp_count: usize =
        state.lag_reg.lag_by_group.read().await.iter().map(|(_, gwl)| gwl.lag_by_topic_partition.len()).sum();
    let metric_types_count: usize = 3;
    let headers_footers_count: usize = metric_types_count * 2;
    let metrics_count: usize = tp_count * metric_types_count;
    let mut metrics_vec: Vec<String> = Vec::with_capacity(metrics_count + headers_footers_count);

    // ----------------------------------------------------------- METRIC: consumer_partition_offset
    metrics::consumer_partition_offset::append_headers(&mut metrics_vec);
    for (g, gwl) in state.lag_reg.lag_by_group.read().await.iter() {
        for (tp, lwo) in gwl.lag_by_topic_partition.iter() {
            metrics::consumer_partition_offset::append_metric(
                &cluster_id,
                g,
                tp.topic.as_ref(),
                tp.partition,
                lwo.owner.as_ref(),
                lwo.lag.as_ref(),
                &mut metrics_vec,
            );
        }
    }
    metrics_vec.push(String::new());

    // ------------------------------------------------------- METRIC: consumer_partition_lag_offset
    metrics::consumer_partition_lag_offset::append_headers(&mut metrics_vec);
    for (g, gwl) in state.lag_reg.lag_by_group.read().await.iter() {
        for (tp, lwo) in gwl.lag_by_topic_partition.iter() {
            metrics::consumer_partition_lag_offset::append_metric(
                &cluster_id,
                g,
                tp.topic.as_ref(),
                tp.partition,
                lwo.owner.as_ref(),
                lwo.lag.as_ref(),
                &mut metrics_vec,
            );
        }
    }
    metrics_vec.push(String::new());

    // ------------------------------------------------- METRIC: consumer_partition_lag_milliseconds
    metrics::consumer_partition_lag_milliseconds::append_headers(&mut metrics_vec);
    for (g, gwl) in state.lag_reg.lag_by_group.read().await.iter() {
        for (tp, lwo) in gwl.lag_by_topic_partition.iter() {
            metrics::consumer_partition_lag_milliseconds::append_metric(
                &cluster_id,
                g,
                tp.topic.as_ref(),
                tp.partition,
                lwo.owner.as_ref(),
                lwo.lag.as_ref(),
                &mut metrics_vec,
            );
        }
    }
    metrics_vec.push(String::new());

    //
    //
    // TODO `kcl_kafka_consumer_partition_earliest_available_offset` NO TIMESTAMP
    //   LABELS: cluster_id?, topic, partition, member_id, member_host, member_client_id
    //   HELP: Earliest consumable offset available to consumers of the given topic partition.
    //
    // TODO `kcl_kafka_consumer_partition_latest_available_offset` NO TIMESTAMP
    //   LABELS: cluster_id?, topic, partition, member_id, member_host, member_client_id
    //   HELP: Latest consumable offset available to consumers of the given topic partition.
    //
    // TODO `kcl_kafka_consumer_partition_earliest_tracked_offset`
    //   LABELS: cluster_id?, group, topic, partition, member_id, member_host, member_client_id
    //   HELP: Earliest tracked offset, used to estimate time lag of the given group for this specific topic partition.
    //
    // TODO `kcl_kafka_consumer_partition_latest_tracked_offset`
    //   LABELS: cluster_id?, group, topic, partition, member_id, member_host, member_client_id
    //   HELP: Latest tracked offset, used to estimate time lag of the given group for this specific topic partition.
    //
    // --- CLUSTER METRICS ---
    //
    // TODO `kcl_consumer_groups_total`
    //   LABELS: cluster_id?
    //
    // TODO `kcl_consumer_group_members_total`
    //   LABELS: cluster_id?
    //
    // TODO `kcl_cluster_status_brokers_total`
    //   LABELS: cluster_id?
    //
    // TODO `kcl_cluster_status_topics_total`
    //   LABELS: cluster_id?
    //
    // TODO `kcl_cluster_status_partitions_total`
    //   LABELS: cluster_id?
    //
    // --- KCL INTERNAL METRICS ---
    //
    // TODO `kcl_consumer_groups_poll_time_seconds`
    //   HELP: Time taken to fetch information about all consumer groups in the cluster.
    //   LABELS: cluster_id?
    //
    // TODO `kcl_cluster_status_poll_time_ms`
    //   HELP: Time taken to fetch information about the composition of the cluster (brokers, topics, partitions).
    //   LABELS: cluster_id?
    //
    // TODO `kcl_partitions_watermark_offsets_poll_time_ms`
    //   HELP: Time taken to fetch earliest/latest (watermark) offsets of all the topic partitions of the cluster.
    //   LABELS: cluster_id?

    (status, headers, metrics_vec.join("\n"))
}
