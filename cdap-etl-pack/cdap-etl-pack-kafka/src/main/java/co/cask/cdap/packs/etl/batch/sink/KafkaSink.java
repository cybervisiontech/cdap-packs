/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.packs.etl.batch.sink;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.packs.etl.BaseKafkaSink;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
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
