package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.kafka.BaseKafkaTest;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.MapReduceManager;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;

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
    args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, "userDetails1");
    Schema inSchema = setSourceSchema(args);

    // transformation configuration
    args.put(Constants.Batch.Transformation.ARG_TRANSFORMATION_TYPE, DefaultSchemaMapping.class.getName());
    setTransformationSchemaAndMapping(args, inSchema);

    // sink configuration
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, KafkaSink.class.getName());
    args.put(Constants.Batch.Sink.Kafka.ARG_KAFKA_ZOOKEEPER_QUORUM, zkConnectionStr);
    args.put(Constants.Batch.Sink.Kafka.ARG_KAFKA_TOPIC, "topic1");

    testApp(BatchETLToKafkaConfiguredWithArgs.class, args, "topic1", "userDetails1");
  }

  @Test
  public void testConfigurationWithCode() throws Exception {
    testApp(BatchETLToKafkaConfiguredWithCode.class, Collections.<String, String>emptyMap(), "topic2", "userDetails2");
  }

  private void testApp(Class<? extends AbstractApplication> app, Map<String, String> args, String topic, String tableName)
    throws TimeoutException, InterruptedException {
    ApplicationManager appMngr = deployApplication(app);

    DataSetManager<Table> table = appMngr.getDataSet(tableName);
    table.get().put(new Put("fooKey").add("userId", "55").add("firstName", "jack").add("lastName", "brown"));
    table.get().put(new Put("barKey").add("userId", "49").add("firstName", "jim").add("lastName", "smith"));
    table.get().put(new Put("bazKey").add("userId", "300").add("firstName", "alex").add("lastName", "roberts"));
    table.flush();

    MapReduceManager mr = appMngr.startMapReduce("ETLMapReduce", args);
    mr.waitForFinish(2, TimeUnit.MINUTES);

    Map<Integer, Map<String, byte[]>> expected = ImmutableMap.of(
      55, mapOf(55, "jack", "brown"),
      49, mapOf(49, "jim", "smith"),
      300, mapOf(300, "alex", "roberts")
    );
    verifyKafkaResults(topic, expected);
  }
}
