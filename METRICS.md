# Metrics exposed by Kommitted

## Namespace

Following Prometheus [metric and label naming](https://prometheus.io/docs/practices/naming/)
good practices, Kommitted uses a namespace that identifies the metrics it creates unequivocally.
But in the interest of keeping things short, `kmtd` it is.
Each metrics is hence named `kmtd_<METRIC NAME>`.

## Metrics

Below is the list of the current Metrics exposed by Kommitted.

### Consumer Metrics

<dl>
  <dt><code>kmtd_kafka_consumer_partition_lag_milliseconds</code></dt>
  <dd>
    <b>Description:</b> <i>The time difference (time lag) between when the latest offset was produced and the latest consumed offset was consumed, by the consumer of the topic partition, expressed in milliseconds. NOTE: '-1, -1' means 'unknown'.</i><br/>
    <b>Labels:</b> <code>cluster_id, group, topic, partition, member_id, member_host, member_client_id</code><br/>
    <b>Type:</b> <code>gauge</code><br/>
    <b>Timestamped:</b> <code>true</code>
  </dd>
</dl>

<dl>
  <dt><code>kmtd_kafka_consumer_partition_lag_offset</code></dt>
  <dd>
    <b>Description:</b> <i>The difference (lag) between the last produced offset and the last consumed offset, by the consumer of the topic partition. NOTE: '0, -1' means 'unknown'.</i><br/>
    <b>Labels:</b> <code>cluster_id, group, topic, partition, member_id, member_host, member_client_id</code><br/>
    <b>Type:</b> <code>gauge</code><br/>
    <b>Timestamped:</b> <code>true</code>
  </dd>
</dl>

<dl>
  <dt><code>kmtd_kafka_consumer_partition_offset</code></dt>
  <dd>
    <b>Description:</b> <i>The last consumed offset by the consumer of the topic partition. NOTE: '0, -1' means 'unknown'.</i><br/>
    <b>Labels:</b> <code>cluster_id, group, topic, partition, member_id, member_host, member_client_id</code><br/>
    <b>Type:</b> <code>gauge</code><br/>
    <b>Timestamped:</b> <code>true</code>
  </dd>
</dl>

### Topic Partition Metrics

<dl>
  <dt><code>kmtd_kafka_partition_earliest_available_offset</code></dt>
  <dd>
    <b>Description:</b> <i>Earliest offset available to consumers of the topic partition.</i><br/>
    <b>Labels:</b> <code>cluster_id, topic, partition</code><br/>
    <b>Type:</b> <code>gauge</code><br/>
    <b>Timestamped:</b> <code>false</code>
  </dd>
</dl>

<dl>
  <dt><code>kmtd_kafka_partition_latest_available_offset</code></dt>
  <dd>
    <b>Description:</b> <i>Latest offset available to consumers of the topic partition.</i><br/>
    <b>Labels:</b> <code>cluster_id, topic, partition</code><br/>
    <b>Type:</b> <code>gauge</code><br/>
    <b>Timestamped:</b> <code>false</code>
  </dd>
</dl>

### Topic Partition Offset Tracking Metrics

<dl>
  <dt><code>kmtd_kafka_partition_earliest_tracked_offset</code></dt>
  <dd>
    <b>Description:</b> <i>Earliest offset tracked to estimate the lag of consumers of the topic partition.</i><br/>
    <b>Labels:</b> <code>cluster_id, topic, partition</code><br/>
    <b>Type:</b> <code>gauge</code><br/>
    <b>Timestamped:</b> <code>true</code>
  </dd>
</dl>

<dl>
  <dt><code>kmtd_kafka_partition_latest_tracked_offset</code></dt>
  <dd>
    <b>Description:</b> <i>Latest offset tracked to estimate the lag of consumers of the topic partition.</i><br/>
    <b>Labels:</b> <code>cluster_id, topic, partition</code><br/>
    <b>Type:</b> <code>gauge</code><br/>
    <b>Timestamped:</b> <code>true</code>
  </dd>
</dl>

## Labels

Each metrics has some or all of the following labels applied; what labels applies
depends on the level of specificity of each metric.

| Specificity ⬇️ |               Name | Definition                                               |
|:--------------:|-------------------:|:---------------------------------------------------------|
|     Least      |       `cluster_id` | Identifier of the Kafka Cluster                          |
|      More      |            `topic` | Name of the Topic                                        |
|      More      |        `partition` | (Numeric) identifier of the Topic Partition              |
|      More      |            `group` | Name of the Consumer Group                               |
|      Most      |        `member_id` | Identifier of a Member in the Consumer Group             |
|      Most      |      `member_host` | Host of a Member in the Consumer Group                   |
|      Most      | `member_client_id` | Configured `client.id` of a Member in the Consumer Group |
