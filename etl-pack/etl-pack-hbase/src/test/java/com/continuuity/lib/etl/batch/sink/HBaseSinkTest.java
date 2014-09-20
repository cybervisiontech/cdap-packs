package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.batch.sink.BatchETLToHBaseConfiguredWithArgs;
import com.continuuity.lib.hbase.HBase96Test;
import com.continuuity.lib.hbase.HBaseTestBase;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.MapReduceManager;
import com.continuuity.test.ReactorTestBase;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class HBaseSinkTest extends ReactorTestBase {
  private static final String CF = "family";
  private static final String TABLE = "table";
  private static String zkConnectionAddress;
  private static int zkClientPort;
  private static HBaseTestBase hBaseTestBase;

  @BeforeClass
  public static void setupETLHBaseSinkTest() throws Exception {
    hBaseTestBase = new HBase96Test();
    hBaseTestBase.startHBase();
    zkConnectionAddress = hBaseTestBase.getZkConnectionString();
    zkClientPort = hBaseTestBase.zkCluster.getClientPort();
  }

  @AfterClass
  public static void cleanupETLHBaseSinkTest() throws Exception {
    hBaseTestBase.stopHBase();
  }

  @Test
  public void testConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // sink configuration
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, HBaseSink.class.getName());
    args.put(Constants.Batch.Sink.HBase.ARG_ROW_KEY_FIELD, "user_id");
    args.put(Constants.Batch.Sink.HBase.ARG_TABLE_NAME, TABLE);
    args.put(Constants.Batch.Sink.HBase.ARG_TABLE_FAMILY, CF);
    args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_CLIENT_PORT, String.valueOf(zkClientPort));
    args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_QUORUM, zkConnectionAddress.split(":")[0]);
    args.put(Constants.Batch.Sink.HBase.ARG_HBASE_ZOOKEEPER_PARENT_NODE, "/hbase");

    testApp(BatchETLToHBaseConfiguredWithArgs.class, args);
  }

  // todo: test configured by code

  private void testApp(Class<? extends AbstractApplication> app, Map<String, String> args) throws Exception {

    // ETL: mr job takes input from table dataset and outputs into HBase table using simple schema mapping transform

    ApplicationManager appMngr = deployApplication(app);

    DataSetManager<Table> table = appMngr.getDataSet("userDetails3");
    table.get().put(new Put("fooKey").add("userId", "55").add("firstName", "jack").add("lastName", "brown"));
    table.flush();

    MapReduceManager mr = appMngr.startMapReduce("ETLMapReduce", args);
    mr.waitForFinish(2, TimeUnit.MINUTES);

    // verify
    HTable hTable = hBaseTestBase.getHTable(Bytes.toBytes(TABLE));
    byte[] row = Bytes.toBytes(55);
    Get get = new Get(row);
    Result result = hTable.get(get);

    Assert.assertFalse(result.isEmpty());
    Assert.assertEquals(55, Bytes.toInt(result.getRow()));
    Assert.assertEquals(55, Bytes.toInt(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("user_id"))));
    Assert.assertEquals("jack", Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("first_name"))));
    Assert.assertEquals("brown", Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("last_name"))));
  }
}
