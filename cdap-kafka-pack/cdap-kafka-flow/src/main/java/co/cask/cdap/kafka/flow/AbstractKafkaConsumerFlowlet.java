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

import co.cask.cdap.api.annotation.Tick;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FailurePolicy;
import co.cask.cdap.api.flow.flowlet.FailureReason;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.InputContext;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import org.apache.twill.kafka.client.TopicPartition;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractKafkaConsumerFlowlet<K, V> extends AbstractFlowlet {

  private static final Charset UTF_8 = Charset.forName("UTF-8");
  private static final Function<KafkaConsumerInfo, Long> CONSUMER_TO_OFFSET = new Function<KafkaConsumerInfo, Long>() {
    @Override
    public Long apply(KafkaConsumerInfo input) {
      return input.getReadOffset();
    }
  };

  private Function<ByteBuffer, K> keyDecoder;
  private Function<ByteBuffer, V> valueDecoder;
  private boolean processValueOnly;
  private KafkaConfig kafkaConfig;
  private Map<TopicPartition, KafkaConsumerInfo> consumerInfos;

  /**
   * Initialize this {@link Flowlet}. Child class must call this method explicitly when overriding it.
   */
  @Override
  public void initialize(FlowletContext context) throws Exception {
    super.initialize(context);

    // Configure kafka
    DefaultKafkaConsumerConfigurer kafkaConfigurer = new DefaultKafkaConsumerConfigurer();
    configureKafka(kafkaConfigurer);

    if (kafkaConfigurer.getZookeeper() == null && kafkaConfigurer.getBrokers() == null) {
      throw new IllegalStateException("Kafka not configured. Must provide either zookeeper or broker list.");
    }

    kafkaConfig = new KafkaConfig(kafkaConfigurer.getZookeeper(), kafkaConfigurer.getBrokers());
    consumerInfos = createConsumerInfos(kafkaConfigurer.getTopicPartitions());

    Type superType = TypeToken.of(getClass()).getSupertype(AbstractKafkaConsumerFlowlet.class).getType();

    // Tries to detect Key and Value type for creating decoder for them
    if (superType instanceof ParameterizedType) {
      // Extract Key and Value types
      Type[] typeArgs = ((ParameterizedType) superType).getActualTypeArguments();

      // Parameter type arguments of AbstractKafkaConsumerFlowlet must be 2
      keyDecoder = createDecoder(typeArgs[0]);
      valueDecoder = createDecoder(typeArgs[1]);
    }
    processValueOnly = isProcessValueOnly();

    // TODO: Initialize readOffset
  }

  /**
   * A {@link Tick} method that triggered periodically by the Flow system to poll messages from Kafka.
   * The default poll delay is 100 milliseconds. This method can be overridden to provide different delay value.
   * <br/>
   * E.g. to override with 1 second instead:
   *
   * <pre>
   * {@literal @}Override
   * {@literal @}Tick(delay = 1, unit = TimeUnit.SECONDS)
   * public void pollMessages() {
   *   super.pollMessages();
   * }
   * </pre>
   */
  @Tick(delay = 100, unit = TimeUnit.MILLISECONDS)
  public void pollMessages() throws Exception {
    for (KafkaConsumerInfo info : consumerInfos.values()) {
      Iterator<KafkaMessage> iterator = readMessages(info);
      while (iterator.hasNext()) {
        KafkaMessage message = iterator.next();

        // Process the message
        if (processValueOnly) {
          processMessage(decodeValue(message.getPayload()));
        } else {
          processMessage(decodeKey(message.getKey()), decodeValue(message.getPayload()));
        }

        // Update the read offset
        info.setReadOffset(message.getNextOffset());
      }
    }

    saveReadOffsets(Maps.transformValues(consumerInfos, CONSUMER_TO_OFFSET));
  }

  @Override
  public void onSuccess(Object input, InputContext inputContext) {
    super.onSuccess(input, inputContext);

    // Input object is null for @Tick method
    if (input != null) {
      return;
    }

    for (KafkaConsumerInfo info : consumerInfos.values()) {
      info.commitReadOffset();
    }
  }

  @Override
  public FailurePolicy onFailure(Object input, InputContext inputContext, FailureReason reason) {
    if (input == null) {
      for (KafkaConsumerInfo info : consumerInfos.values()) {
        info.rollbackReadOffset();
      }
    }
    return FailurePolicy.RETRY;
  }

  /**
   * Override to configure Kafka consumer.
   */
  protected abstract void configureKafka(KafkaConsumerConfigurer configurer);

  /**
   * Override to read messages from Kafka.
   *
   * @param consumerInfo Contains information about where to fetch messages from
   * @return An {@link Iterator} containing sequence of messages read from Kafka. The first message must
   *         has offset no earlier than the {@link KafkaConsumerInfo#getReadOffset()} as given in the parameter.
   */
  protected abstract Iterator<KafkaMessage> readMessages(KafkaConsumerInfo consumerInfo);

  /**
   * Returns the read offsets to start with for the given {@link TopicPartition}.
   *
   * @param topicPartition The topic and partition that needs the start offset
   * @return The starting offset. Returning special value {@code -1L} for latest offset or {@code -2L} for earliest
   *         offset.
   */
  protected abstract long getBeginOffset(TopicPartition topicPartition);

  /**
   * Persists read offsets for all topic-partition that this Flowlet consumes from Kafka.
   */
  protected abstract void saveReadOffsets(Map<TopicPartition, Long> offsets);

  /**
   * Returns a Kafka configuration.
   */
  protected final KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }

  /**
   * Override this method if interested both key and value of a message read from Kafka. If both
   * this and the {@link AbstractKafkaConsumerFlowlet#processMessage(Object)} are overridden, only this
   * method will be called.
   *
   * @param key Key decoded from the message
   * @param value Value decoded from the message
   */
  protected void processMessage(K key, V value) throws Exception {
    // No-op by default.
  }

  /**
   * Override this method if only interested in the value of a message read from Kafka.
   *
   * @param value Value decoded from the message
   */
  protected void processMessage(V value) throws Exception {
    // No-op by default.
  }

  /**
   * Override this method to provide custom decoding of message key.
   *
   * @param buffer The bytes representing the key in the Kafka message
   * @return The decoded key
   */
  protected K decodeKey(ByteBuffer buffer) {
    if (keyDecoder != null) {
      return keyDecoder.apply(buffer);
    }
    throw new IllegalStateException("No decoder for decoding message key");
  }

  /**
   * Override this method to provide custom decoding of message value.
   *
   * @param buffer The bytes representing the value in the Kafka message
   * @return The decoded value
   */
  protected V decodeValue(ByteBuffer buffer) {
    if (valueDecoder != null) {
      return valueDecoder.apply(buffer);
    }
    throw new IllegalStateException("No decoder for decoding message value");
  }

  /**
   * Creates a decoder for decoding {@link ByteBuffer} for known type. It supports
   *
   * <pre>
   * - String (assuming UTF-8)
   * - byte[]
   * - ByteBuffer
   * </pre>
   *
   * @param type type to decode to
   * @param <T> Type of the decoded type
   * @return A {@link Function} that decode {@link ByteBuffer} into the given type or
   *         {@code null} if the type is not supported.
   */
  @SuppressWarnings("unchecked")
  private <T> Function<ByteBuffer, T> createDecoder(final Type type) {
    if (String.class.equals(type)) {
      return (Function<ByteBuffer, T>) createStringDecoder();
    }
    if (ByteBuffer.class.equals(type)) {
      return (Function<ByteBuffer, T>) createByteBufferDecoder();
    }
    if (type instanceof GenericArrayType && byte.class.equals(((GenericArrayType) type).getGenericComponentType())) {
      return (Function<ByteBuffer, T>) createBytesDecoder();
    }
    return null;
  }

  private Function<ByteBuffer, String> createStringDecoder() {
    return new Function<ByteBuffer, String>() {
      @Override
      public String apply(ByteBuffer input) {
        return UTF_8.decode(input).toString();
      }
    };
  }

  private Function<ByteBuffer, ByteBuffer> createByteBufferDecoder() {
    return new Function<ByteBuffer, ByteBuffer>() {
      @Override
      public ByteBuffer apply(ByteBuffer input) {
        return input;
      }
    };
  }

  private Function<ByteBuffer, byte[]> createBytesDecoder() {
    return new Function<ByteBuffer, byte[]>() {
      @Override
      public byte[] apply(ByteBuffer input) {
        byte[] bytes = new byte[input.remaining()];
        input.mark();
        input.get(bytes);
        input.reset();
        return bytes;
      }
    };
  }

  private Map<TopicPartition, KafkaConsumerInfo> createConsumerInfos(Map<TopicPartition, Integer> config) {
    ImmutableMap.Builder<TopicPartition, KafkaConsumerInfo> consumers = ImmutableMap.builder();

    for (Map.Entry<TopicPartition, Integer> entry : config.entrySet()) {
      consumers.put(entry.getKey(),
                    new KafkaConsumerInfo(entry.getKey(), entry.getValue(), getBeginOffset(entry.getKey())));
    }
    return consumers.build();
  }

  private boolean isProcessValueOnly() {
    try {
      // Try to get the processMessage(K key, V value) method from the current implementation class.
      getClass().getDeclaredMethod("processMessage", Object.class, Object.class);
      return false;
    } catch (NoSuchMethodException e) {
      return true;
    }
  }
}
