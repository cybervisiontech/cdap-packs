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

package co.cask.cdap.packs.etl.kafka;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.schema.Schema;
import co.cask.cdap.test.TestBase;
import co.cask.common.io.ByteBufferInputStream;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.twill.internal.kafka.EmbeddedKafkaServer;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.internal.utils.Networks;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static co.cask.cdap.packs.etl.TestConstants.getInSchema;
import static co.cask.cdap.packs.etl.TestConstants.getMapping;
import static co.cask.cdap.packs.etl.TestConstants.getOutSchema;

/**
 *
 */
public class BaseKafkaTest extends TestBase {
  private static final Logger LOG = LoggerFactory.getLogger(BaseKafkaTest.class);
  private static final Gson GSON = new Gson();

  private static InMemoryZKServer zkServer;
  private static ZKClientService zkClientService;
  private static EmbeddedKafkaServer kafkaServer;

  protected static KafkaClientService kafkaClientService;
  protected static String zkConnectionStr;

  @BeforeClass
  public static void startKafka() throws IOException {
    zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();
    zkConnectionStr = zkServer.getConnectionStr();
    // workaround for classloader not being able to see the static variable
    System.setProperty("zk.connection.str", zkConnectionStr);
    zkClientService = ZKClientService.Builder.of(zkConnectionStr).build();
    zkClientService.startAndWait();
    kafkaClientService = new ZKKafkaClientService(zkClientService);
    kafkaClientService.startAndWait();

    Properties kafkaConfig = generateKafkaConfig();
    kafkaServer = new EmbeddedKafkaServer(kafkaConfig);
    kafkaServer.startAndWait();
  }

  @AfterClass
  public static void stopKafka() {
    kafkaServer.stopAndWait();
    zkServer.stopAndWait();
  }

  protected void verifyKafkaResults(String topic,
                                    Map<Integer, Map<String, byte[]>> expected) throws InterruptedException {

    final Map<Integer, Map<String, byte[]>> received = Maps.newHashMap();
    final CountDownLatch latch = new CountDownLatch(1);

    kafkaClientService.getConsumer().prepare().addFromBeginning(topic, 0)
      .consume(new KafkaConsumer.MessageCallback() {

        @Override
        public void onReceived(Iterator<FetchedMessage> messages) {
          while (messages.hasNext()) {
            FetchedMessage message = messages.next();
            ByteBufferInputStream byteIn = new ByteBufferInputStream(message.getPayload());
            ObjectInputStream in = null;
            try {
              in = new ObjectInputStream(byteIn);
              Map<String, byte[]> data = (Map<String, byte[]>) in.readObject();
              received.put(Bytes.toInt(data.get("user_id")), data);
            } catch (IOException e) {
              LOG.error("Error reading data.", e);
            } catch (ClassNotFoundException e) {
              LOG.error("Error decoding map.", e);
            } finally {
              if (in != null) {
                try {
                  in.close();
                } catch (IOException e) {
                  LOG.error("Error closing input stream", e);
                }
              }
            }
          }
        }

        @Override
        public void finished() {
          LOG.info("finished");
          latch.countDown();
        }
      });

    latch.await(5, TimeUnit.SECONDS);
    Assert.assertEquals(expected.size(), received.size());
    for (Map.Entry<Integer, Map<String, byte[]>> entry : expected.entrySet()) {
      Integer userId = entry.getKey();
      Map<String, byte[]> expectedFields = entry.getValue();
      Map<String, byte[]> actualFields = received.get(userId);
      Assert.assertEquals(expectedFields.size(), actualFields.size());
      for (Map.Entry<String, byte[]> expectedField : expectedFields.entrySet()) {
        Assert.assertTrue(actualFields.containsKey(expectedField.getKey()));
        Assert.assertTrue(Arrays.equals(expectedField.getValue(), actualFields.get(expectedField.getKey())));
      }
    }
  }

  private static Properties generateKafkaConfig() throws IOException {
    int port = Networks.getRandomPort();
    Preconditions.checkState(port > 0, "Failed to get random port.");

    Properties prop = new Properties();
    prop.setProperty("broker.id", "1");
    prop.setProperty("port", Integer.toString(port));
    prop.setProperty("num.network.threads", "2");
    prop.setProperty("num.io.threads", "2");
    prop.setProperty("socket.send.buffer.bytes", "1048576");
    prop.setProperty("socket.receive.buffer.bytes", "1048576");
    prop.setProperty("socket.request.max.bytes", "104857600");
    prop.setProperty("log.dir", tmpFolder.newFolder().getAbsolutePath());
    prop.setProperty("num.partitions", "1");
    prop.setProperty("log.flush.interval.messages", "10000");
    prop.setProperty("log.flush.interval.ms", "1000");
    prop.setProperty("log.retention.hours", "1");
    prop.setProperty("log.segment.bytes", "536870912");
    prop.setProperty("log.cleanup.interval.mins", "1");
    prop.setProperty("zookeeper.connect", zkConnectionStr);
    prop.setProperty("zookeeper.connection.timeout.ms", "1000000");

    return prop;
  }

  protected Schema setSourceSchema(Map<String, String> args) {
    Schema inSchema = getInSchema();
    args.put(Constants.Batch.Source.ARG_SOURCE_SCHEMA, GSON.toJson(inSchema));
    return inSchema;
  }

  protected Schema setTransformationSchemaAndMapping(Map<String, String> args, Schema inSchema) {
    args.put(Constants.Transformation.Schema.ARG_INPUT_SCHEMA, GSON.toJson(inSchema));
    Schema outSchema = getOutSchema();
    args.put(Constants.Transformation.Schema.ARG_OUTPUT_SCHEMA, GSON.toJson(outSchema));
    args.put(Constants.Transformation.SchemaMapping.ARG_MAPPING, GSON.toJson(getMapping()));

    return outSchema;
  }

  protected Map<String, byte[]> mapOf(Integer userId, String firstName, String lastName) {
    return ImmutableMap.of(
      "user_id", Bytes.toBytes(userId),
      "first_name", Bytes.toBytes(firstName),
      "last_name", Bytes.toBytes(lastName));
  }

}
