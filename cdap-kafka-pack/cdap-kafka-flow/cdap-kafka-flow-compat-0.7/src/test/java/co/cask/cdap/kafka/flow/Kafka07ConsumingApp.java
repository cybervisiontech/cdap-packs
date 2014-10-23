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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Application for testing Kafka consuming flowlet for Kafka 0.7.
 */
public class Kafka07ConsumingApp extends KafkaConsumingApp {

  private final Flowlet sourceFlowlet = new KafkaSource();

  @Override
  protected Flowlet getKafkaSourceFlowlet() {
    return sourceFlowlet;
  }

  /**
   * A flowlet to poll from Kafka.
   */
  public static final class KafkaSource extends Kafka07ConsumerFlowlet<String> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

    @UseDataSet("kafkaOffsets")
    private KeyValueTable offsetStore;

    private boolean failed;
    private OutputEmitter<String> emitter;

    @Override
    protected void processMessage(String value) throws Exception {
      LOG.info("Message: {}", value);
      if (value.equals("Failure")) {
        if (!failed) {
          failed = true;
          // Intentionally throw exception for the first time that it sees a failure message.
          // The second time will ignore it, not emitting to downstream
          throw new IllegalStateException("Failed with value: " + value);
        }
        return;
      }
      emitter.emit(value);
    }

    @Override
    protected void configureKafka(KafkaConfigurer configurer) {
      Map<String, String> runtimeArgs = getContext().getRuntimeArguments();
      if (runtimeArgs.containsKey("kafka.zookeeper")) {
        configurer.setZooKeeper(runtimeArgs.get("kafka.zookeeper"));
      } else if (runtimeArgs.containsKey("kafka.brokers")) {
        configurer.setBrokers(runtimeArgs.get("kafka.brokers"));
      }

      configurer.addTopicPartition(runtimeArgs.get("kafka.topic"), 0);
      configurer.addTopicPartition(runtimeArgs.get("kafka.topic"), 1);
    }

    @Override
    protected KeyValueTable getOffsetStore() {
      return offsetStore;
    }
  }
}
