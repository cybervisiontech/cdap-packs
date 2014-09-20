package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DictionarySinkTest extends ReactorTestBase {

  @Test
  public void testConfigurationWithCode() throws Exception {
    testApp(RealtimeETLToDictionaryConfiguredWithCode.class, Collections.<String, String>emptyMap());
  }

  @Test
  public void testConfigurationWithArgs() throws Exception {
    ImmutableMap<String, String> args =
        ImmutableMap.of(Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_NAME, "myDict",
                        Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "fname");
    testApp(RealtimeETLToDictionaryConfiguredWithArgs.class, args);
  }

  protected void testApp(Class<? extends AbstractApplication> app, Map<String, String> args) throws Exception {

    ApplicationManager appMngr = deployApplication(app);

    StreamWriter sw = appMngr.getStreamWriter("filesStream");
    sw.send(ImmutableMap.of("fname", "file1", "size", "1024"), "ignored_body");

    FlowManager flow = appMngr.startFlow("ETLFlow", args);
    RuntimeMetrics terminalMetrics = RuntimeStats.getFlowletMetrics(app.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 5, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);

    // verify
    DataSetManager<DictionaryDataSet> dict = appMngr.getDataSet(Constants.DICTIONARY_DATASET);
    Assert.assertEquals("1024", Bytes.toString(dict.get().get("myDict", "file1".getBytes(Charsets.UTF_8), "size")));
    Assert.assertEquals("file1", Bytes.toString(dict.get().get("myDict", "file1".getBytes(Charsets.UTF_8), "fname")));

    flow.stop();

  }
}
