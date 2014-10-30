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

import co.cask.cdap.api.flow.flowlet.FlowletContext;
import com.google.common.base.Splitter;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Service;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.TopicPartition;
import org.apache.twill.zookeeper.RetryStrategies;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.twill.zookeeper.ZKClientServices;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractKafka08ConsumerFlowlet<K, V> extends AbstractKafkaConsumerFlowlet<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKafka08ConsumerFlowlet.class);
  private static final int SO_TIMEOUT = 5 * 1000;           // 5 seconds.

  private Map<TopicPartition, SimpleConsumer> kafkaConsumers;
  private ZKClientService zkClient;
  private BrokerService brokerService;

  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);
    kafkaConsumers = Maps.newHashMap();

    String kafkaZKConnect = getKafkaConfig().getZookeeper();
    if (kafkaZKConnect != null) {
      zkClient = ZKClientServices.delegate(
        ZKClients.reWatchOnExpire(
          ZKClients.retryOnFailure(ZKClientService.Builder.of(kafkaZKConnect).build(),
                                   RetryStrategies.fixDelay(2, TimeUnit.SECONDS))
        ));
      zkClient.startAndWait();

      brokerService = createBrokerService(zkClient);
      brokerService.startAndWait();
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    if (kafkaConsumers != null) {
      for (SimpleConsumer consumer : kafkaConsumers.values()) {
        try {
          consumer.close();
        } catch (Throwable t) {
          LOG.error("Exception when closing Kafka consumer.", t);
        }
      }
    }

    if (brokerService != null) {
      stopService(brokerService);
    }
    if (zkClient != null) {
      stopService(zkClient);
    }
  }

  @Override
  protected Iterator<KafkaMessage> readMessages(KafkaConsumerInfo consumerInfo) {
    final TopicPartition topicPartition = consumerInfo.getTopicPartition();
    String topic = topicPartition.getTopic();
    int partition = topicPartition.getPartition();

    final SimpleConsumer consumer = getConsumer(consumerInfo);
    if (consumer == null) {
      return Iterators.emptyIterator();
    }
    long readOffset = consumerInfo.getReadOffset();
    if (readOffset < 0) {
      readOffset = getReadOffset(consumer, topic, partition, readOffset);
    }

    FetchRequest fetchRequest = new FetchRequestBuilder()
      .clientId(consumer.clientId())
      .addFetch(topic, partition, readOffset, consumerInfo.getFetchSize())
      .build();

    FetchResponse response = consumer.fetch(fetchRequest);

    if (response.hasError()) {
      short errorCode = response.errorCode(topic, partition);
      LOG.error("Failed to fetch from broker {}:{} for topic-partition {}-{} with error code {}",
                consumer.host(), consumer.port(), topic, partition, errorCode);
      if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
        // Get the earliest offset
        long earliest = getReadOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime());
        // If the current read offset is smaller than the earliest one, use the earliest offset in next fetch
        if (readOffset < earliest) {
          consumerInfo.setReadOffset(earliest);
        } else {
          // Otherwise the read offset must be larger than the latest (otherwise it won't have the out of range error)
          consumerInfo.setReadOffset(getReadOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime()));
        }
      } else {
        // For other type of error, close the consumer so that a new one will be created in next iteration
        removeConsumer(consumerInfo);
      }
      return Iterators.emptyIterator();
    }

    // Returns an Iterator of message
    final long fetchReadOffset = readOffset;
    final Iterator<MessageAndOffset> messages = response.messageSet(topic, partition).iterator();
    return new AbstractIterator<KafkaMessage>() {
      @Override
      protected KafkaMessage computeNext() {
        while (messages.hasNext()) {
          MessageAndOffset messageAndOffset = messages.next();
          if (messageAndOffset.offset() < fetchReadOffset) {
            // Older message read (which is valid in Kafka), skip it.
            continue;
          }
          Message message = messageAndOffset.message();
          return new KafkaMessage(topicPartition, messageAndOffset.offset(),
                                  messageAndOffset.nextOffset(), message.key(), message.payload());
        }
        return endOfData();
      }
    };
  }

  private void stopService(Service service) {
    try {
      service.stopAndWait();
    } catch (Throwable t) {
      LOG.error("Failed when stopping service {}", service, t);
    }
  }

  private long getReadOffset(SimpleConsumer consumer, String topic, int partition, long time) {
    OffsetRequest offsetRequest = new OffsetRequest(
      ImmutableMap.of(new TopicAndPartition(topic, partition), new PartitionOffsetRequestInfo(time, 1)),
      kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

    OffsetResponse response = consumer.getOffsetsBefore(offsetRequest);
    if (response.hasError()) {
      LOG.error("Failed to fetch offset from broker {}:{} for topic-partition {}-{} with error code {}",
                consumer.host(), consumer.port(), topic, partition, response.errorCode(topic, partition));
      return 0L;
    }
    return response.offsets(topic, partition)[0];
  }

  /**
   * Returns a {@link SimpleConsumer} for the given consumer info or {@code null} if no leader broker is currently
   * available.
   */
  private SimpleConsumer getConsumer(KafkaConsumerInfo consumerInfo) {
    TopicPartition topicPartition = consumerInfo.getTopicPartition();
    SimpleConsumer consumer = kafkaConsumers.get(topicPartition);
    if (consumer != null) {
      return consumer;
    }

    InetSocketAddress leader = getLeader(topicPartition.getTopic(), topicPartition.getPartition());
    if (leader == null) {
      return null;
    }
    String consumerName = String.format("%s-%d-kafka-consumer", getName(), getContext().getInstanceId());
    consumer = new SimpleConsumer(leader.getHostName(), leader.getPort(),
                                  SO_TIMEOUT, consumerInfo.getFetchSize(), consumerName);
    kafkaConsumers.put(topicPartition, consumer);

    return consumer;
  }

  private void removeConsumer(KafkaConsumerInfo consumerInfo) {
    SimpleConsumer consumer = kafkaConsumers.remove(consumerInfo.getTopicPartition());
    if (consumer != null) {
      consumer.close();
    }
  }

  /**
   * Creates a Kafka {@link BrokerService}.
   */
  private BrokerService createBrokerService(ZKClient zkClient) throws Exception {
    // TODO: Hacky way to construct ZKBrokerService from Twill as it's a protected class
    // Need Twill update to resolve this hack
    Class<? extends BrokerService> brokerClass = loadBrokerServiceClass(BrokerService.class.getClassLoader());
    Constructor<? extends BrokerService> constructor = brokerClass.getDeclaredConstructor(ZKClient.class);
    constructor.setAccessible(true);
    return constructor.newInstance(zkClient);
  }


  @SuppressWarnings("unchecked")
  private <T> Class<T> loadBrokerServiceClass(ClassLoader classLoader) throws ClassNotFoundException {
    return (Class<T>) classLoader.loadClass("org.apache.twill.internal.kafka.client.ZKBrokerService");
  }

  private InetSocketAddress getLeader(String topic, int partition) {
    // If BrokerService is available, it will use information from ZooKeeper
    if (brokerService != null) {
      BrokerInfo brokerInfo = brokerService.getLeader(topic, partition);
      return new InetSocketAddress(brokerInfo.getHost(), brokerInfo.getPort());
    }

    // Otherwise use broker list to discover leader
    return findLeader(getKafkaConfig().getBrokers(), topic, partition);
  }

  /**
   * Finds the leader broker address for the given topic partition.
   *
   * @return the address for the leader broker or null if no leader is currently available.
   */
  private InetSocketAddress findLeader(String brokers, String topic, int partition) {
    // Splits the broker list of format "host:port,host:port" into a map<host, port>
    Map<String, String> brokerMap = Splitter.on(',').withKeyValueSeparator(":").split(brokers);

    // Go through the broker list and try to find a leader for the given topic partition.
    for (Map.Entry<String, String> broker : brokerMap.entrySet()) {
      try {
        SimpleConsumer consumer = new SimpleConsumer(broker.getKey(), Integer.parseInt(broker.getValue()), SO_TIMEOUT,
                                                     KafkaConsumerConfigurer.DEFAULT_FETCH_SIZE, "leaderLookup");
        try {
          TopicMetadataRequest request = new TopicMetadataRequest(ImmutableList.of(topic));
          TopicMetadataResponse response = consumer.send(request);
          for (TopicMetadata topicData : response.topicsMetadata()) {
            for (PartitionMetadata partitionData : topicData.partitionsMetadata()) {
              if (partitionData.partitionId() == partition) {
                Broker leader = partitionData.leader();
                return new InetSocketAddress(leader.host(), leader.port());
              }
            }
          }

        } finally {
          consumer.close();
        }
      } catch (Exception e) {
        LOG.error("Failed to communicate with broker {}:{} for leader lookup for topic-partition {}-{}",
                  broker.getKey(), broker.getValue(), topic, partition, e);
      }
    }
    return null;
  }
}
