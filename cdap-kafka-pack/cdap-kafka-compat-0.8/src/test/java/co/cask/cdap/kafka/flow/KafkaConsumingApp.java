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

package co.cask.cdap.kafka.flow;

import co.cask.cdap.api.annotation.ProcessInput;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 */
public class KafkaConsumingApp extends AbstractApplication {

  @Override
  public void configure() {
    addFlow(new KafkaConsumingFlow());
    createDataset("kafkaOffsets", KeyValueTable.class);
  }

  public static final class KafkaConsumingFlow implements Flow {

    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("KafkaConsumingFlow")
        .setDescription("")
        .withFlowlets()
          .add(new KafkaSource())
          .add(new DataSink())
        .connect()
          .from(new KafkaSource()).to(new DataSink())
        .build();
    }
  }

  /**
   * A flowlet to poll from Kafka.
   */
  public static final class KafkaSource extends AbstractKafka08ConsumerFlowlet<byte[], String> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    @UseDataSet("kafkaOffsets")
    private KeyValueTable offsetStore;

    private OutputEmitter<String> emitter;

    @Override
    protected void processMessage(String value) throws Exception {
      LOG.info("Message: {}", value);
      emitter.emit(value);
    }

    @Override
    protected void configureKafka(KafkaConsumerConfigurer configurer) {
      Map<String, String> runtimeArgs = getContext().getRuntimeArguments();
      if (runtimeArgs.containsKey("kafka.zookeeper")) {
        configurer.setZooKeeper(runtimeArgs.get("kafka.zookeeper"));
      } else if (runtimeArgs.containsKey("kafka.brokers")) {
        configurer.setBrokers(runtimeArgs.get("kafka.brokers"));
      }

      configurer.addTopicPartition(runtimeArgs.get("kafka.topic"), 0);
    }

    @Override
    protected KeyValueTable getOffsetStore() {
      return offsetStore;
    }
  }

  public static final class DataSink extends AbstractFlowlet {

    private static final Logger LOG = LoggerFactory.getLogger(DataSink.class);

    @ProcessInput
    public void process(String string) {
      LOG.info("Received: {}", string);
    }
  }
}
