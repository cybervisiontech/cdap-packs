package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.lib.etl.BaseKafkaSink;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Writes data out to Kafka.
 */
public class KafkaSink extends BaseKafkaSink<FlowletContext> implements RealtimeSink {

  public KafkaSink() {
  }

  public KafkaSink(String zkQuorum, String topic, String partitionKey) {
    super(zkQuorum, topic, partitionKey);
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (zkQuorum != null) {
      args.put(Constants.Realtime.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM, zkQuorum);
    }
    if (topic != null) {
      args.put(Constants.Realtime.Sink.Kafka.ARG_KAFKA_TOPIC, topic);
    }
    if (partitionField != null) {
      args.put(Constants.Realtime.Sink.Kafka.ARG_KAFKA_PARTITION_FIELD, partitionField);
    }
    return args;
  }

  @Override
  public void initialize(FlowletContext context) {
    zkQuorum = Programs.getArgOrProperty(context, Constants.Realtime.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM);
    topic = Programs.getArgOrProperty(context, Constants.Realtime.Sink.Kafka.ARG_KAFKA_TOPIC);
    partitionField = Programs.getArgOrProperty(context, Constants.Realtime.Sink.Kafka.ARG_KAFKA_PARTITION_FIELD);
    startZKAndKafka();
  }

  @Override
  public void write(Record value) throws Exception {
    writeRecord(value);
  }
}
