/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.packs.etl.realtime.sink;

import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.packs.etl.etl.BaseKafkaSink;
import co.cask.cdap.packs.etl.etl.Constants;
import co.cask.cdap.packs.etl.etl.Programs;
import co.cask.cdap.packs.etl.etl.Record;
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
