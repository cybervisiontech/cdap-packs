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

/**
 *
 */
public interface KafkaConsumerConfigurer {

  /**
   * Default message fetch size in bytes when making Kafka fetch request.
   */
  static final int DEFAULT_FETCH_SIZE = 1048576;  // 1M

  /**
   * Sets the ZooKeeper quorum string that Kafka is running with. If this is set, then ZooKeeper will be used
   * for discovery of Kafka brokers, regardless of what's being set by {@link #setBrokers(String)}.
   */
  void setZooKeeper(String zookeeper);

  /**
   * Sets the Kafka broker list. The format of the broker list is based on the Kafka version.
   *
   * In Kafka 0.8, broker list is a comma separated list of "host:port".
   * <br>
   * In Kafka 0.7, broker list is a comma separate list of "brokerId:host:port"
   */
  void setBrokers(String brokers);

  /**
   * Adds a topic partition to consume message from. Same as calling
   *
   * {@link #addTopicPartition(String, int, int) addTopicPartition(topic, partition, DEFAULT_FETCH_SIZE)}
   *
   * @param topic name of the Kafka topic
   * @param partition partition number
   */
  void addTopicPartition(String topic, int partition);

  /**
   * Adds a topic partition to consumer message from, using the given fetch size for each fetch request.
   *
   * @param topic name of the Kafka topic
   * @param partition partition number
   * @param fetchSize maximum number of bytes to fetch per request
   */
  void addTopicPartition(String topic, int partition, int fetchSize);
}
