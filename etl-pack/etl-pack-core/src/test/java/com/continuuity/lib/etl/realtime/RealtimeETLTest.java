package com.continuuity.lib.etl.realtime;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.TestConstants;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.realtime.sink.DictionarySink;
import com.continuuity.lib.etl.realtime.source.SchemaSource;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Testing {@link RealtimeETL} pipeline as a whole
 */
public class RealtimeETLTest extends ReactorTestBase {
  private static final Gson GSON = new Gson();
  protected static final String USER = "1";
  protected static final String FIRST_NAME = "alex";
  protected static final String LAST_NAME = "baranau";

  @Test
  public void testAppConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // source configuration
    args.put(Constants.Realtime.Source.ARG_SOURCE_TYPE, SchemaSource.class.getName());
    args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, "userDetails");
    args.put(Constants.Realtime.Transformation.ARG_TRANSFORMATION_TYPE, DefaultSchemaMapping.class.getName());
    Schema inSchema = setSourceSchema(args);

    // transformation configuration
    setTransformationSchemaAndMapping(args, inSchema);

    // sink configuration
    args.put(Constants.Realtime.Sink.ARG_SINK_TYPE, DictionarySink.class.getName());
    args.put(Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_NAME, "users");
    args.put(Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "user_id");

    testApp(RealtimeETLConfigureByArgs.class, args);
  }

  @Test
  public void testAppConfiguredByCode() throws Exception {
    testApp(RealtimeETLConfigureByCode.class, Collections.<String, String>emptyMap());
  }

  protected void testApp(Class<? extends AbstractApplication> app, Map<String, String> args) throws Exception {
    // Simple ETL pipeline: data from the stream transformed with simple filed mapping logic and written to dictionary

    ApplicationManager appMngr = deployApplication(app);

    StreamWriter sw = appMngr.getStreamWriter("userDetailsStream");
    sw.send(USER + "," + FIRST_NAME + "," + LAST_NAME);

    FlowManager flow = appMngr.startFlow("ETLFlow", args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 5, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);

    // verify
    DataSetManager<DictionaryDataSet> dictionary = appMngr.getDataSet(Constants.DICTIONARY_DATASET);
    // NOTE: using int value
    Assert.assertEquals(FIRST_NAME, Bytes.toString(dictionary.get().get("users", Bytes.toBytes(1), "first_name")));
    Assert.assertEquals(LAST_NAME, Bytes.toString(dictionary.get().get("users", Bytes.toBytes(1), "last_name")));

    flow.stop();
  }

  private Schema setSourceSchema(Map<String, String> args) {
    Schema inSchema = TestConstants.getInSchema();
    args.put(Constants.Realtime.Source.ARG_SOURCE_SCHEMA, GSON.toJson(inSchema));
    return inSchema;
  }

  private Schema setTransformationSchemaAndMapping(Map<String, String> args, Schema inSchema) {
    args.put(Constants.Transformation.Schema.ARG_INPUT_SCHEMA, GSON.toJson(inSchema));
    Schema outSchema = TestConstants.getOutSchema();
    args.put(Constants.Transformation.Schema.ARG_OUTPUT_SCHEMA, GSON.toJson(outSchema));
    args.put(Constants.Transformation.SchemaMapping.ARG_MAPPING, GSON.toJson(TestConstants.getMapping()));

    return outSchema;
  }
}
