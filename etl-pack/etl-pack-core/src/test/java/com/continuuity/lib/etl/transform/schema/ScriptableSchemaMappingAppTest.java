package com.continuuity.lib.etl.transform.schema;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.realtime.RealtimeETL;
import com.continuuity.lib.etl.realtime.sink.DictionarySink;
import com.continuuity.lib.etl.realtime.source.SchemaSource;
import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.FieldType;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScriptableSchemaMappingAppTest extends ReactorTestBase {

  @Test
  public void testConfigurationWithCode() throws Exception {
    testApp(RealtimeETLUsingScriptableMapping.class, Collections.<String, String>emptyMap());
  }

  protected void testApp(Class<? extends AbstractApplication> app, Map<String, String> args) throws Exception {

    ApplicationManager appMngr = deployApplication(app);

    // populate dictionary
    DataSetManager<DictionaryDataSet> dict = appMngr.getDataSet(Constants.DICTIONARY_DATASET);
    dict.get().write("users", Bytes.toBytes(55), ImmutableMap.of("firstName", Bytes.toBytes("jack")));
    dict.flush();

    StreamWriter sw = appMngr.getStreamWriter("usersStream");
    sw.send("55");

    FlowManager flow = appMngr.startFlow("ETLFlow", args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 5, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(5);

    // verify
    dict = appMngr.getDataSet(Constants.DICTIONARY_DATASET);
    Assert.assertEquals("jack", Bytes.toString(dict.get().get("myDict", Bytes.toBytes(55), "first_name")));

    flow.stop();

  }
}
