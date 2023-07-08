// TODO Re-enable clippy
#![allow(clippy::all)]

use std::{net::SocketAddr, sync::Arc};

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
    lag_reg: Arc<LagRegister>,
}

pub async fn init(lag_reg: Arc<LagRegister>, shutdown_token: CancellationToken) {
    let state = HttpServiceState {
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

    // As defined by Prometheus: https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md#basic-info
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; version=0.0.4"));

    // TODO Determine a good capacity in advance
    //   Given the number of topic partitions
    //   multiple for the number of metrics we create per topic-partition
    let mut metrics_vec: Vec<String> = Vec::with_capacity(100);

    for (_group_name, group_lag) in state.lag_reg.lag_by_group.read().await.iter() {
        for (_topic_partition, _lag) in group_lag.lag_by_topic_partition.iter() {
            // TODO expose the ID of the cluster (as `cluster_id`) as a way to differentiate metrics coming
            //   from different Kafka clusters into the same Prometheus.
            //   This might be just echoing a Command Line argument set by the user, if the `cluster_id` can't
            //   be procured by querying the cluster itself.
            //
            // --- LAG and PARTITION METRICS TO EXPOSE ---
            //
            // TODO `kcl_kafka_consumer_partition_offset`
            //   LABELS: cluster_id?, group, topic, partition, member_id, member_host, member_client_id
            //   HELP: The last consumed offset by the given group for this specific topic partition.
            //
            // TODO `kcl_kafka_consumer_partition_lag_offset`
            //   LABELS: cluster_id?, group, topic, partition, member_id, member_host, member_client_id
            //   HELP: The difference (lag) between the last produced offset and the last consumed offset, by the given group for this specific topic partition.
            //
            // TODO `kcl_kafka_consumer_partition_lag_seconds`
            //   LABELS: cluster_id?, group, topic, partition, member_id, member_host, member_client_id
            //   HELP: The time difference (time lag) between when the latest offset was produced and the latest consumed offset was consumed, by the given group for this specific topic partition, expressed in seconds.
            //
            //
            // TODO `kcl_kafka_consumer_max_lag_offset`
            //   LABELS: cluster_id?, group, topic, partition, member_id, member_host, member_client_id
            //   HELP: The max of the difference (lag) between the last produced offset and the last consumed offset, by the given group for this specific topic partition.
            //
            // TODO `kcl_kafka_consumer_max_lag_seconds`
            //   LABELS: cluster_id?, group, topic, partition, member_id, member_host, member_client_id
            //   HELP: The max of the time difference (time lag) between when the latest offset was produced and the latest consumed offset was consumed, by the given group for this specific topic partition, expressed in seconds.
            //
            //
            // TODO `kcl_kafka_group_sum_lag_offset`
            //   LABELS: cluster_id?, group
            //   HELP: The sum of the difference (lag) between the last produced offset and last consumed offset, for all the topic partition consumed by the given group.
            //
            // TODO `kcl_kafka_group_sum_lag_seconds`
            //   LABELS: cluster_id?, group
            //   HELP: The sum of the time difference (time lag) between when the latest offset was produced and the latest consumed offset was consumed, for all the topic partition consumed by the given group.
            //
            //
            // TODO `kcl_kafka_partition_earliest_offset`
            //   LABELS: cluster_id?, topic, partition, member_id, member_host, member_client_id
            //   HELP: Earliest consumable offset available to consumers of the given topic partition.
            //
            // TODO `kcl_kafka_partition_latest_offset`
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
        }
    }

    metrics_vec.push(format!("metric1{{test=1}}"));
    metrics_vec.push(format!("metric2{{x=2}}"));
    metrics_vec.push(format!("metric3"));

    (status, headers, metrics_vec.join("\n"))
}
