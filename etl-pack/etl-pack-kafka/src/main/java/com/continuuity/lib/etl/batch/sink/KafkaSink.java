package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.lib.etl.BaseKafkaSink;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * Writes data out to Kafka.
 */
public class KafkaSink extends BaseKafkaSink<MapReduceContext> implements MapReduceSink {

  public KafkaSink() {
  }

  public KafkaSink(String zkQuorum, String topic, String partitionKey) {
    super(zkQuorum, topic, partitionKey);
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (zkQuorum != null) {
      args.put(Constants.Batch.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM, zkQuorum);
    }
    if (topic != null) {
      args.put(Constants.Batch.Sink.Kafka.ARG_KAFKA_TOPIC, topic);
    }
    if (partitionField != null) {
      args.put(Constants.Batch.Sink.Kafka.ARG_KAFKA_PARTITION_FIELD, partitionField);
    }
    return args;
  }

  @Override
  public void initialize(MapReduceContext context) {
    zkQuorum = Programs.getArgOrProperty(context, Constants.Batch.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM);
    topic = Programs.getArgOrProperty(context, Constants.Batch.Sink.Kafka.ARG_KAFKA_TOPIC);
    partitionField = Programs.getArgOrProperty(context, Constants.Batch.Sink.Kafka.ARG_KAFKA_PARTITION_FIELD);
    startZKAndKafka();
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    Programs.checkArgOrPropertyIsSet(context, Constants.Batch.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM);
    Programs.checkArgOrPropertyIsSet(context, Constants.Batch.Sink.Kafka.ARG_KAFKA_TOPIC);

    Job job = context.getHadoopJob();
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setOutputKeyClass(Integer.class);
    job.setOutputValueClass(Integer.class);
  }

  @Override
  public void write(Mapper.Context context, Record record) throws IOException, InterruptedException {
    writeRecord(record);
  }
}
