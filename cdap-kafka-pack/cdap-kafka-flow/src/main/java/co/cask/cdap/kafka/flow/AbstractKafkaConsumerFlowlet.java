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
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.AbstractFlowlet;
import co.cask.cdap.api.flow.flowlet.FailurePolicy;
import co.cask.cdap.api.flow.flowlet.FailureReason;
import co.cask.cdap.api.flow.flowlet.Flowlet;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.InputContext;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * @param <KEY> Type of message key
 * @param <PAYLOAD> Type of message value
 * @param <OFFSET> Type of offset object
 */
public abstract class AbstractKafkaConsumerFlowlet<KEY, PAYLOAD, OFFSET> extends AbstractFlowlet {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractKafkaConsumerFlowlet.class);

  private final Function<KafkaConsumerInfo<OFFSET>, OFFSET> consumerToOffset =
    new Function<KafkaConsumerInfo<OFFSET>, OFFSET>() {
    @Override
    public OFFSET apply(KafkaConsumerInfo<OFFSET> input) {
      return input.getReadOffset();
    }
  };

  private Function<ByteBuffer, KEY> keyDecoder;
  private Function<ByteBuffer, PAYLOAD> payloadDecoder;
  private boolean processPayloadOnly;
  private KafkaConfig kafkaConfig;
  private Map<TopicPartition, KafkaConsumerInfo<OFFSET>> consumerInfos;

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

    // Tries to detect Key and Payload type for creating decoder for them
    if (superType instanceof ParameterizedType) {
      // Extract Key and Payload types
      Type[] typeArgs = ((ParameterizedType) superType).getActualTypeArguments();

      // Parameter type arguments of AbstractKafkaConsumerFlowlet must be 2
      keyDecoder = createDecoder(typeArgs[0]);
      payloadDecoder = createDecoder(typeArgs[1]);
    }
    processPayloadOnly = isProcessPayloadOnly();
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
    for (KafkaConsumerInfo<OFFSET> info : consumerInfos.values()) {
      Iterator<KafkaMessage<OFFSET>> iterator = readMessages(info);
      while (iterator.hasNext()) {
        KafkaMessage<OFFSET> message = iterator.next();

        // Process the message
        if (processPayloadOnly) {
          processMessage(decodePayload(message.getPayload()));
        } else {
          processMessage(decodeKey(message.getKey()), decodePayload(message.getPayload()));
        }

        // Update the read offset
        info.setReadOffset(message.getNextOffset());
      }
    }

    saveReadOffsets(Maps.transformValues(consumerInfos, consumerToOffset));
  }

  @Override
  public void onSuccess(Object input, InputContext inputContext) {
    super.onSuccess(input, inputContext);

    // Input object is null for @Tick method
    if (input != null) {
      return;
    }

    for (KafkaConsumerInfo<OFFSET> info : consumerInfos.values()) {
      info.commitReadOffset();
    }
  }

  @Override
  public FailurePolicy onFailure(Object input, InputContext inputContext, FailureReason reason) {
    if (input == null) {
      for (KafkaConsumerInfo<OFFSET> info : consumerInfos.values()) {
        info.rollbackReadOffset();
      }
    }
    return FailurePolicy.RETRY;
  }

  /**
   * Override to return a {@link KeyValueTable} for storing consumer offsets.
   */
  protected KeyValueTable getOffsetStore() {
    return null;
  }


  /**
   * Override to configure Kafka consumer. This method will be called during {@link #initialize(FlowletContext)} phase,
   * hence it has access to {@link FlowletContext} through the {@link #getContext()} method.
   */
  protected abstract void configureKafka(KafkaConsumerConfigurer configurer);

  /**
   * Override to read messages from Kafka.
   *
   * @param consumerInfo Contains information about where to fetch messages from
   * @return An {@link Iterator} containing sequence of messages read from Kafka. The first message must
   *         has offset no earlier than the {@link KafkaConsumerInfo#getReadOffset()} as given in the parameter.
   */
  protected abstract Iterator<KafkaMessage<OFFSET>> readMessages(KafkaConsumerInfo<OFFSET> consumerInfo);

  /**
   * Returns the read offsets to start with for the given {@link TopicPartition}.
   */
  protected abstract OFFSET getBeginOffset(TopicPartition topicPartition);

  /**
   * Persists read offsets for all topic-partition that this Flowlet consumes from Kafka.
   */
  protected abstract void saveReadOffsets(Map<TopicPartition, OFFSET> offsets);

  /**
   * Returns a Kafka configuration.
   */
  protected final KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }

  /**
   * Override this method if interested both key and payload of a message read from Kafka. If both
   * this and the {@link AbstractKafkaConsumerFlowlet#processMessage(Object)} are overridden, only this
   * method will be called.
   *
   * @param key Key decoded from the message
   * @param payload Payload decoded from the message
   */
  protected void processMessage(KEY key, PAYLOAD payload) throws Exception {
    // No-op by default.
  }

  /**
   * Override this method if only interested in the payload of a message read from Kafka.
   *
   * @param payload Payload decoded from the message
   */
  protected void processMessage(PAYLOAD payload) throws Exception {
    // No-op by default.
  }

  /**
   * Override this method to provide custom decoding of message key.
   *
   * @param buffer The bytes representing the key in the Kafka message
   * @return The decoded key
   */
  protected KEY decodeKey(ByteBuffer buffer) {
    if (keyDecoder != null) {
      return keyDecoder.apply(buffer);
    }
    throw new IllegalStateException("No decoder for decoding message key");
  }

  /**
   * Override this method to provide custom decoding of message payload.
   *
   * @param buffer The bytes representing the payload in the Kafka message
   * @return The decoded payload
   */
  protected PAYLOAD decodePayload(ByteBuffer buffer) {
    if (payloadDecoder != null) {
      return payloadDecoder.apply(buffer);
    }
    throw new IllegalStateException("No decoder for decoding message payload");
  }

  /**
   * Stops a {@link Service} and waits for the completion. If there is exception during stop, it will get logged.
   */
  protected final void stopService(Service service) {
    try {
      service.stopAndWait();
    } catch (Throwable t) {
      LOG.error("Failed when stopping service {}", service, t);
    }
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
        return Charsets.UTF_8.decode(input).toString();
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

  private Map<TopicPartition, KafkaConsumerInfo<OFFSET>> createConsumerInfos(Map<TopicPartition, Integer> config) {
    ImmutableMap.Builder<TopicPartition, KafkaConsumerInfo<OFFSET>> consumers = ImmutableMap.builder();

    for (Map.Entry<TopicPartition, Integer> entry : config.entrySet()) {
      consumers.put(entry.getKey(),
                    new KafkaConsumerInfo<OFFSET>(entry.getKey(), entry.getValue(), getBeginOffset(entry.getKey())));
    }
    return consumers.build();
  }

  private boolean isProcessPayloadOnly() {
    try {
      // Try to get the processMessage(KEY, PAYLOAD) method from the current implementation class.
      getClass().getDeclaredMethod("processMessage", Object.class, Object.class);
      return false;
    } catch (NoSuchMethodException e) {
      return true;
    }
  }
}
