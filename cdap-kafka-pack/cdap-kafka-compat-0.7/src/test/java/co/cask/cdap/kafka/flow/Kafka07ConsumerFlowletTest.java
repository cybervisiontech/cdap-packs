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

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class Kafka07ConsumerFlowletTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static int kafkaPort;
  private static InMemoryZKServer zkServer;
  private static EmbeddedKafkaServer kafkaServer;
  private static ZKClientService zkClient;

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
  }

  @AfterClass
  public static void tearDown() {
    zkClient.stopAndWait();

    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  @Test
  public void test() throws Exception {
    Properties prop = new Properties();
    prop.setProperty("zk.connect", zkServer.getConnectionStr());
    prop.setProperty("serializer.class", "kafka.serializer.StringEncoder");

    ProducerConfig prodConfig = new ProducerConfig(prop);
    Producer<String, String> producer = new Producer<String, String>(prodConfig);

    for (int i = 0; i < 10; i++) {
      producer.send(new ProducerData<String, String>("test", "Message " + i));
    }

    KafkaBrokerCache cache = new KafkaBrokerCache(zkClient);
    cache.startAndWait();
    TimeUnit.SECONDS.sleep(3);
    System.out.println(cache.getBrokers("test", 3));
    System.out.println(cache.getPartitionSize("test"));
    cache.stopAndWait();

//
//    SimpleConsumer consumer = new SimpleConsumer("localhost", kafkaPort,
//                                                 100000, KafkaConsumerConfigurer.DEFAULT_FETCH_SIZE);
//    long[] offsets = consumer.getOffsetsBefore("test", 0, -2L, 1);
//    System.out.println(Arrays.toString(offsets));

//    FetchRequest fetchRequest = new FetchRequest("test", 0, 0, KafkaConsumerConfigurer.DEFAULT_FETCH_SIZE);
//    ByteBufferMessageSet messageSet = consumer.fetch(fetchRequest);
//    System.out.println("Initial offset: " + messageSet.getInitialOffset());
//    for (MessageAndOffset messageAndOffset : messageSet) {
//      Message message = messageAndOffset.message();
//      System.out.println("message offset: " + messageAndOffset.offset());
//      System.out.println(Charsets.UTF_8.decode(message.payload()));
//    }
  }

  private static Properties generateKafkaConfig(String zkConnectStr, int port, File logDir) {
    Properties prop = new Properties();
    prop.setProperty("log.dir", logDir.getAbsolutePath());
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("brokerid", "1");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.retention.hours", "24");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("zk.connect", zkConnectStr);
    prop.setProperty("zk.connectiontimeout.ms", "1000000");
    prop.setProperty("num.partitions", "3");
    return prop;
  }
}
