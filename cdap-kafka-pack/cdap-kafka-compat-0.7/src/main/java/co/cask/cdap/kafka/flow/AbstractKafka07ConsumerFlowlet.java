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

import java.util.Map;

/**
 *
 * @param <KEY>
 * @param <VALUE>
 */
public abstract class AbstractKafka07ConsumerFlowlet<KEY, VALUE>
                extends AbstractKafkaConsumerFlowlet<KEY, VALUE, MessageOffset> {

  @Override
  protected void saveReadOffsets(Map<TopicPartition, MessageOffset> offsets) {

  }

  @Override
  protected MessageOffset getBeginOffset(TopicPartition topicPartition) {
    return null;
  }
}
