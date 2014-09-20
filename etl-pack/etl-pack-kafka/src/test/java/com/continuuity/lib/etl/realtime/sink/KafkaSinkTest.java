package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.kafka.BaseKafkaTest;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class KafkaSinkTest extends BaseKafkaTest {

  @Test
  public void testAppConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // source configuration
    args.put(Constants.Batch.Source.ARG_SOURCE_TYPE, TableSource.class.getName());
    args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, "userDetails");
    Schema inSchema = setSourceSchema(args);

    // transformation configuration
    args.put(Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE, DefaultSchemaMapping.class.getName());
    setTransformationSchemaAndMapping(args, inSchema);

    // sink configuration
    // todo change to Realtime.Sink?
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, com.continuuity.lib.etl.batch.sink.KafkaSink.class.getName());
    args.put(Constants.Realtime.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM, zkConnectionStr);
    args.put(Constants.Realtime.Sink.Kafka.ARG_KAFKA_TOPIC, "topic1");

    testApp(RealtimeETLToKafkaConfiguredWithArgs.class, args, "topic1", "stream1");
  }

  @Test
  public void testConfigurationWithCode() throws Exception {
    testApp(RealtimeETLToKafkaConfiguredWithCode.class, Collections.<String, String>emptyMap(), "topic2", "stream2");
  }

  private void testApp(Class<? extends AbstractApplication> app, Map<String, String> args, String topic, String stream)
    throws TimeoutException, InterruptedException, IOException {

    ApplicationManager appMngr = deployApplication(app);

    StreamWriter sw = appMngr.getStreamWriter(stream);
    sw.send("55,jack,brown");
    sw.send("49,jim,smith");
    sw.send("300,alex,roberts");

    FlowManager flow = appMngr.startFlow("ETLFlow", args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(2, 10, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);
    flow.stop();

    Map<Integer, Map<String, byte[]>> expected = ImmutableMap.of(
      55, mapOf(55, "jack", "brown"),
      49, mapOf(49, "jim", "smith"),
      300, mapOf(300, "alex", "roberts")
    );
    verifyKafkaResults(topic, expected);
  }
}
