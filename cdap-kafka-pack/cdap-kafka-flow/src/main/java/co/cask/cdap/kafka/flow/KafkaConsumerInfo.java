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

import org.apache.twill.kafka.client.TopicPartition;

/**
 * Helper class to carry information about Kafka consumer of a particular topic partition.
 */
public final class KafkaConsumerInfo {
  private final TopicPartition topicPartition;
  private final int fetchSize;
  private long readOffset;
  private long pendingReadOffset;

  KafkaConsumerInfo(TopicPartition topicPartition, int fetchSize, long readOffset) {
    this.topicPartition = topicPartition;
    this.fetchSize = fetchSize;
    this.readOffset = readOffset;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public long getReadOffset() {
    return pendingReadOffset > readOffset ? pendingReadOffset : readOffset;
  }

  public void setReadOffset(long readOffset) {
    this.pendingReadOffset = readOffset;
  }

  void commitReadOffset() {
    if (pendingReadOffset > readOffset) {
      readOffset = pendingReadOffset;
      pendingReadOffset = Long.MIN_VALUE;
    }
  }

  void rollbackReadOffset() {
    pendingReadOffset = Long.MIN_VALUE;
  }
}
