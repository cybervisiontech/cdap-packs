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

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Unit-test for Kafka consuming flowlet for Kafka 0.7.
 */
public class Kafka07ConsumerFlowletTest extends KafkaConsumerFlowletTestBase {

  private static ZKClientService zkClient;

  @BeforeClass
  public static void setup() throws IOException {
    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();
  }

  @AfterClass
  public static void tearDown() {
    zkClient.stopAndWait();
  }

  @Override
  protected Class<? extends KafkaConsumingApp> getApplication() {
    return Kafka07ConsumingApp.class;
  }

  @Override
  protected void sendMessage(String topic, Map<String, String> messages) {
    Properties prop = new Properties();
    prop.setProperty("zk.connect", zkServer.getConnectionStr());
    prop.setProperty("serializer.class", "kafka.serializer.StringEncoder");

    ProducerConfig prodConfig = new ProducerConfig(prop);

    List<ProducerData<String, String>> outMessages = Lists.newArrayList();
    for (Map.Entry<String, String> entry : messages.entrySet()) {
      outMessages.add(new ProducerData<String, String>(topic, entry.getKey(), ImmutableList.of(entry.getValue())));
    }
    Producer<String, String> producer = new Producer<String, String>(prodConfig);
    producer.send(outMessages);
  }

  @Override
  protected Map<String, String> getRuntimeArgs(String topic, boolean preferZK) {
    Map<String, String> args = Maps.newHashMap(super.getRuntimeArgs(topic, preferZK));

    // Always use ZK in Kafka 0.7
    args.remove("kafka.brokers");
    args.put("kafka.zookeeper", zkServer.getConnectionStr());
    return args;
  }
}
