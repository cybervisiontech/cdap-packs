package com.continuuity.lib.etl.realtime.source;

import com.continuuity.api.common.Bytes;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.realtime.RealtimeETL;
import com.continuuity.lib.etl.realtime.sink.DictionarySink;
import com.continuuity.lib.etl.transform.IdentityTransformation;
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

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MetadataSourceTest extends ReactorTestBase {

  @Test
  public void test() throws Exception {
    ApplicationManager appMngr = deployApplication(ETLWithMetadataSource.class);

    StreamWriter sw = appMngr.getStreamWriter("filesStream");
    sw.send(ImmutableMap.of("filename", "file1", "size", "1024"), "ignored_body");

    FlowManager flow = appMngr.startFlow("ETLFlow");
    RuntimeMetrics terminalMetrics =
      RuntimeStats.getFlowletMetrics(ETLWithMetadataSource.class.getSimpleName(), "ETLFlow", "ETLFlowlet");
    terminalMetrics.waitForinput(1, 5, TimeUnit.SECONDS);
    TimeUnit.SECONDS.sleep(1);

    // verify
    DataSetManager<DictionaryDataSet> dict = appMngr.getDataSet(Constants.DICTIONARY_DATASET);
    Assert.assertEquals("1024", Bytes.toString(dict.get().get("myDict", "file1".getBytes(Charsets.UTF_8), "size")));

    flow.stop();
  }
}
