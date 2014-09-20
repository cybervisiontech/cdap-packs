package com.continuuity.packs.etl;

import com.continuuity.api.data.stream.Stream;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.FlowManager;
import com.continuuity.test.ProcedureManager;
import com.continuuity.test.ReactorTestBase;
import com.continuuity.test.RuntimeMetrics;
import com.continuuity.test.RuntimeStats;
import com.continuuity.test.StreamWriter;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class SimpleDataDictionaryTest extends ReactorTestBase {

  @Test
  public void test() throws Exception {
    try {
      ApplicationManager appManager = deployApplication(TestDataDictionary.class);

      // Starts a flow
      FlowManager flowManager = appManager.startFlow("DataDictionaryProcessor_foo");

      // Write a message to stream
      StreamWriter streamWriter = appManager.getStreamWriter("test");
      streamWriter.send("A:B");  // Second parameter is body and it can be anything.
      TimeUnit.SECONDS.sleep(2);
      flowManager.stop();

      // Start procedure and query for word frequency.
      ProcedureManager procedureManager = appManager.startProcedure("DataDictionaryProcedure_foo");
      String response = procedureManager.getClient().query("get", ImmutableMap.of("key", "A:B"));
      Assert.assertEquals("\"A:B\"", response);
      procedureManager.stop();
    } finally {
      clear();
    }
  }
}
