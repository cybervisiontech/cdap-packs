package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.TestConstants;
import com.continuuity.lib.etl.realtime.RealtimeETL;
import com.continuuity.lib.etl.realtime.sink.RealtimeETLToHBaseConfiguredWithArgs;
import com.continuuity.lib.etl.realtime.sink.RealtimeETLToHBaseConfiguredWithCode;
import com.continuuity.lib.etl.realtime.source.SchemaSource;
import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.FieldType;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;
import com.continuuity.lib.hbase.HBase96Test;
import com.continuuity.lib.hbase.HBaseTestBase;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.continuuity.lib.etl.TestConstants.CF;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

/**
 *
 */
public class HBaseSinkTest extends ReactorTestBase {
  private static String zkConnectionAddress;
  private static int zkClientPort;
  private static HBaseTestBase hBaseTestBase;

  @BeforeClass
  public static void setupETLHBaseSinkTest() throws Exception {
    hBaseTestBase = new HBase96Test();
    hBaseTestBase.startHBase();
    zkConnectionAddress = hBaseTestBase.getZkConnectionString();
    System.setProperty("zk.host", zkConnectionAddress);
    zkClientPort = hBaseTestBase.zkCluster.getClientPort();
    System.setProperty("zk.port", String.valueOf(zkClientPort));
  }

  @AfterClass
  public static void cleanupETLHBaseSinkTest() throws Exception {
    hBaseTestBase.stopHBase();
  }

  @Test
  public void testConfigurationWithArgs() throws Exception {
    // sink configuration
    ImmutableMap<String, String> args = ImmutableMap.<String, String>builder()
      .put(SchemaSink.ARG_SCHEMA, new Gson().toJson(getOutSchema()))
      .put(Constants.Realtime.Sink.HBase.ARG_ROW_KEY_FIELD, "user_id")
      .put(Constants.Realtime.Sink.HBase.ARG_TABLE_NAME, "table1")
      .put(Constants.Realtime.Sink.HBase.ARG_COLFAM, CF)
      .put(Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM, zkConnectionAddress.split(":")[0])
      .put(Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT, String.valueOf(zkClientPort))
      .put(Constants.Realtime.Sink.HBase.ARG_HBASE_ZOOKEEPER_PARENT_NODE, String.valueOf("/hbase")).build();
    testApp(RealtimeETLToHBaseConfiguredWithArgs.class, args, "stream1", "table1");
  }

  @Test
  public void testConfigurationWithCode() throws Exception {
    testApp(RealtimeETLToHBaseConfiguredWithCode.class, Collections.<String, String>emptyMap(), "stream2", "table2");
  }

  protected void testApp(Class<? extends AbstractApplication> app, Map<String, String> args,
                         String streamName, String tableName) throws Exception {

    ApplicationManager appMngr = deployApplication(app);

    StreamWriter sw = appMngr.getStreamWriter(streamName);
    sw.send("55,jack,brown");

    FlowManager flow = appMngr.startFlow("ETLFlow", args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 20, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);

    // verify
    HTable hTable = hBaseTestBase.getHTable(Bytes.toBytes(tableName));
    byte[] row = Bytes.toBytes(55);
    Get get = new Get(row);
    Result result = hTable.get(get);

    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(55, Bytes.toInt(result.getRow()));
    Assert.assertEquals(55, Bytes.toInt(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("user_id"))));
    Assert.assertEquals("jack", Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("first_name"))));
    Assert.assertEquals("brown", Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("last_name"))));

    flow.stop();

  }
}
