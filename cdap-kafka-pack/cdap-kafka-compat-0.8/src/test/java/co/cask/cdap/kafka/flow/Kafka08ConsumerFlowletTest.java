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

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Kafka08ConsumerFlowletTest extends TestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static int kafkaPort;
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static ZKClientService zkClient;
  private static KafkaClientService kafkaClient;

  @BeforeClass
  public static void setup() throws IOException {
    zkServer = InMemoryZKServer.builder().setDataDir(TMP_FOLDER.newFolder()).build();
    zkServer.startAndWait();

    kafkaPort = Networks.getRandomPort();
    kafkaServer = new EmbeddedKafkaServer(generateKafkaConfig(zkServer.getConnectionStr(),
                                                              kafkaPort, TMP_FOLDER.newFolder()));
    kafkaServer.startAndWait();

    zkClient = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
    zkClient.startAndWait();

    kafkaClient = new ZKKafkaClientService(zkClient);
    kafkaClient.startAndWait();
  }

  @AfterClass
  public static void tearDown() {
    kafkaClient.stopAndWait();
    zkClient.stopAndWait();

    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void testFlowlet() throws Exception {
    String topic = "testTopic";
    ApplicationManager appManager = deployApplication(KafkaConsumingApp.class);
    FlowManager flowManager = appManager.startFlow("KafkaConsumingFlow",
                                                   ImmutableMap.of("kafka.brokers", "localhost:" + kafkaPort,
                                                                   "kafka.topic", topic));

    // Publish a message to Kafka, the flow should consume it
    KafkaPublisher publisher = kafkaClient.getPublisher(KafkaPublisher.Ack.ALL_RECEIVED, Compression.NONE);
    publisher.prepare(topic).add(Charsets.UTF_8.encode("Message 1"), 0).send();

    RuntimeMetrics sinkMetrics = RuntimeStats.getFlowletMetrics("KafkaConsumingApp", "KafkaConsumingFlow", "DataSink");
    sinkMetrics.waitForProcessed(1L, 10, TimeUnit.SECONDS);
    flowManager.stop();

    // Publish a message when the flow is not running
    publisher.prepare(topic).add(Charsets.UTF_8.encode("Message 2"), 0).send();

    // Start the flow again (using ZK to discover broker this time)
    RuntimeStats.clearStats("KafkaConsumingApp");


    flowManager = startFlowWithRetry(appManager, "KafkaConsumingFlow",
                                     ImmutableMap.of("kafka.zookeeper", zkServer.getConnectionStr(),
                                                     "kafka.topic", topic), 5);

    TimeUnit.SECONDS.sleep(5);
    flowManager.stop();
  }

  private FlowManager startFlowWithRetry(ApplicationManager appManager,
                                         String flowId, Map<String, String> args, int trials) {

    Throwable failure = null;
    do {
      try {
        if (failure != null) {
          TimeUnit.SECONDS.sleep(1);
        }
        return appManager.startFlow(flowId, args);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      } catch (Throwable t) {
        // Just memorize the failure
        failure = t;
      }
    } while (--trials > 0);

    throw Throwables.propagate(failure);
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("broker.id", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("zookeeper.connect", zkConnectStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");
    prop.setProperty("default.replication.factor", "1");
    return prop;
  }
}
