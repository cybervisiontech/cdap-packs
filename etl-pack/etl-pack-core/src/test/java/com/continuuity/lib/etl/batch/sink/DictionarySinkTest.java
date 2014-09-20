package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.MapReduceManager;
import com.continuuity.test.ReactorTestBase;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class DictionarySinkTest extends ReactorTestBase {
  private static final Gson GSON = new Gson();

  @Test
  public void testConfiguredByArgs() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // sink configuration
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, DictionarySink.class.getName());
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME, "users1");
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "user_id");

    // NOTE: using unique table and dic name to avoid clash with other tests
    testApp(BatchETLToDictionaryConfiguredWithArgs.class, args, "userDetails1", "users1");
  }

  @Test
  public void testConfiguredByCode() throws Exception {
    // NOTE: using unique table and dic name to avoid clash with other tests
    testApp(BatchETLToDictionaryConfiguredWithCode.class, Collections.<String, String>emptyMap(), "userDetails2", "users2");
  }

  private void testApp(Class<? extends AbstractApplication> app,
                       Map<String, String> args, String tableName, String dictionaryName)
    throws TimeoutException, InterruptedException {

    // simple etl: mr job takes input from table dataset and outputs into dictionary dataset using
    //                      identity function (no actual transform)

    ApplicationManager appMngr = deployApplication(app);

    DataSetManager<Table> table = appMngr.getDataSet(tableName);
    table.get().put(new Put("fooKey").add("userId", "23").add("firstName", "jack").add("lastName", "brown"));
    table.flush();

    MapReduceManager mr = appMngr.startMapReduce("ETLMapReduce", args);
    mr.waitForFinish(2, TimeUnit.MINUTES);

    // verify
    DataSetManager<DictionaryDataSet> dictionary = appMngr.getDataSet(Constants.DICTIONARY_DATASET);
    // NOTE: using int value
    Assert.assertEquals(23, Bytes.toInt(dictionary.get().get(dictionaryName, Bytes.toBytes(23), "user_id")));
    Assert.assertEquals("jack", Bytes.toString(dictionary.get().get(dictionaryName, Bytes.toBytes(23), "first_name")));
    Assert.assertEquals("brown", Bytes.toString(dictionary.get().get(dictionaryName, Bytes.toBytes(23), "last_name")));
  }
}
