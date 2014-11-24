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

package co.cask.cdap.packs.etl;

import co.cask.cdap.api.RuntimeContext;
import com.google.common.collect.Maps;
import org.apache.twill.internal.kafka.client.ZKKafkaClientService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for writing to Kafka. Starts up a client service for Zookeeper and Kafka and writes a Map of strings to
 * byte array to a kafka topic.
 */
public class BaseKafkaSink<T extends RuntimeContext> extends AbstractConfigurableProgram<T> {
  private static final byte[] DEFAULT_PARTITION_VAL = new byte[] { 'a' };

  protected KafkaPublisher.Preparer preparer;
  protected String partitionField;
  protected String zkQuorum;
  protected String topic;

  public BaseKafkaSink() {
  }

  public BaseKafkaSink(String zkQuorum, String topic, String partitionField) {
    this.zkQuorum = zkQuorum;
    this.partitionField = partitionField;
    this.topic = topic;
  }

  protected void startZKAndKafka() {
    ZKClientService zkClientService = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(
          ZKClientService.Builder.of(zkQuorum)
            .setSessionTimeout(10000)
            .build(),
          RetryStrategies.exponentialDelay(500, 2000, TimeUnit.MILLISECONDS)
        )
      )
    );
    zkClientService.startAndWait();
    KafkaClientService kafkaClientService = new ZKKafkaClientService(zkClientService);
    kafkaClientService.startAndWait();
    preparer = kafkaClientService.getPublisher(
      KafkaPublisher.Ack.FIRE_AND_FORGET, Compression.NONE).prepare(topic);
  }

  protected void writeRecord(Record record) throws IOException {
    byte[] partitionVal = DEFAULT_PARTITION_VAL;
    if (partitionField != null && record.getFields().contains(partitionField)) {
      partitionVal = record.getValue(partitionField);
    }

    ByteArrayOutputStream byteOut = null;
    ObjectOutputStream objectOut = null;
    try {
      byteOut = new ByteArrayOutputStream();
      objectOut = new ObjectOutputStream(byteOut);
      // ideally developers will be able to plugin in custom encoding logic.
      Map<String, byte[]> fields = Maps.newHashMap();
      for (String field : record.getFields()) {
        fields.put(field, record.getValue(field));
      }
      objectOut.writeObject(fields);
      preparer.add(ByteBuffer.wrap(byteOut.toByteArray()), partitionVal);
      preparer.send();
    } finally {
      if (objectOut != null) {
        objectOut.close();
      }
      if (byteOut != null) {
        byteOut.close();
      }
    }
  }
}
