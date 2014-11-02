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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import org.apache.twill.kafka.client.TopicPartition;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *
 * @param <KEY>
 * @param <VALUE>
 */
public abstract class AbstractKafka07ConsumerFlowlet<KEY, VALUE>
                extends AbstractKafkaConsumerFlowlet<KEY, VALUE, MessageOffset> {

  private ZKClientService zkClient;
  private KafkaBrokerCache kafkaBrokerCache;

  // These two fields are for reusing of byte[] for encoded offset.
  private byte[] lastEncodedOffset;
  private MessageOffset lastOffset;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);

    String kafkaZKConnect = getKafkaConfig().getZookeeper();
    if (kafkaZKConnect == null) {
      throw new IllegalStateException("Must provide ZooKeeper quorum string to consume from Kafka 0.7 cluster");
    }

    zkClient = ZKClientServices.delegate(
      ZKClients.reWatchOnExpire(
        ZKClients.retryOnFailure(ZKClientService.Builder.of(kafkaZKConnect).build(),
                                 RetryStrategies.fixDelay(2, TimeUnit.SECONDS))
      ));
    zkClient.startAndWait();

    kafkaBrokerCache = new KafkaBrokerCache(zkClient);
    kafkaBrokerCache.startAndWait();
  }

  @Override
  public void destroy() {
    super.destroy();
    if (kafkaBrokerCache != null) {
      stopService(kafkaBrokerCache);
    }
    if (zkClient != null) {
      stopService(zkClient);
    }
  }

  /**
   * Returns the beginning offset for the given topic partition. It uses the {@link KeyValueTable} returned
   * by {@link #getOffsetStore()} to lookup information. If no table is provided, this method returns {@code null}.
   *
   * @param topicPartition The topic and partition that needs the start offset
   * @return The starting offset or {@code null} if not able to lookup the offset
   */
  @Override
  protected MessageOffset getBeginOffset(TopicPartition topicPartition) {
    KeyValueTable offsetStore = getOffsetStore();
    if (offsetStore == null) {
      return null;
    }

    byte[] value = offsetStore.read(topicPartition.getTopic() + topicPartition.getPartition());
    // The value contains a long offset and the broker id, hence must be longer than 8-bytes
    if (value == null || value.length <= Bytes.SIZEOF_LONG) {
      return null;
    }

    return decodeOffset(value);
  }

  /**
   * Persists offset for each {@link TopicPartition} to a {@link KeyValueTable} provided by
   * {@link #getOffsetStore()}. The key is simply concatenation of
   * topic and partition and the value is a 8-bytes encoded long of the offset followed by the brokerId which the
   * offset is from. If no dataset is provided, this method is a no-op.
   *
   * @param offsets Map from topic partition to offset to save.
   */
  @Override
  protected void saveReadOffsets(Map<TopicPartition, MessageOffset> offsets) {
    KeyValueTable offsetStore = getOffsetStore();
    if (offsetStore == null) {
      return;
    }

    for (Map.Entry<TopicPartition, MessageOffset> entry : offsets.entrySet()) {
      String key = entry.getKey().getTopic() + entry.getKey().getPartition();
      lastEncodedOffset = encodeOffset(entry.getValue(), lastEncodedOffset, lastOffset);
      lastOffset = entry.getValue();
      offsetStore.write(key, lastEncodedOffset);
    }
  }

  /**
   * Encodes {@link MessageOffset} into 8-bytes long + broker_id (UTF-8).
   *
   * @param offset MessageOffset to encode
   * @param lastEncoded byte array containing the previously encoding result or null
   * @param lastOffset MessageOffset that encoded into lastEncoded or null
   * @return a byte array containing the encoded offset.
   */
  private byte[] encodeOffset(MessageOffset offset, @Nullable byte[] lastEncoded, @Nullable MessageOffset lastOffset) {
    byte[] encodedId;

    if (lastEncoded != null && lastOffset != null) {
      if (offset.getBrokerId().equals(lastOffset.getBrokerId())) {
        // Best case scenario, only need to encode the offset long into the lastEncoded array. No need array is needed.
        Bytes.putLong(lastEncoded, 0, offset.getOffset());
        return lastEncoded;
      }
      // If broker id are different, need to encode the broker id first and see if the size match
      encodedId = Bytes.toBytes(offset.getBrokerId());
      if (encodedId.length != (lastEncoded.length - Bytes.SIZEOF_LONG)) {
        // If encoded broker id size is not the same size as previous one, we have to allocate a new array
        lastEncoded = new byte[Bytes.SIZEOF_LONG + encodedId.length];
      }
    } else {
      // If no result array can be reuse, always need to encode the id and allocate a new array
      encodedId = Bytes.toBytes(offset.getBrokerId());
      lastEncoded = new byte[Bytes.SIZEOF_LONG + encodedId.length];
    }

    // Encode the offset value and copy the encoded broker id
    Bytes.putLong(lastEncoded, 0, offset.getOffset());
    System.arraycopy(encodedId, 0, lastEncoded, Bytes.SIZEOF_LONG, encodedId.length);
    return lastEncoded;
  }

  private MessageOffset decodeOffset(byte[] encodedOffset) {
    long offset = Bytes.toLong(encodedOffset);
    String brokerId = Bytes.toString(encodedOffset, Bytes.SIZEOF_LONG, encodedOffset.length - Bytes.SIZEOF_LONG);

    return new MessageOffset(brokerId, offset);
  }
}
