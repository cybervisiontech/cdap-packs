package com.continuuity.lib.etl.batch;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.batch.sink.DictionarySink;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.FieldType;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;
import com.continuuity.test.ApplicationManager;
import com.continuuity.test.DataSetManager;
import com.continuuity.test.MapReduceManager;
import com.continuuity.test.ReactorTestBase;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.continuuity.lib.etl.TestConstants.getInSchema;
import static com.continuuity.lib.etl.TestConstants.getMapping;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

/**
 *
 */
public class BatchETLTest extends ReactorTestBase {
  private static final Gson GSON = new Gson();

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
    args.put(Constants.Batch.Sink.ARG_SINK_TYPE, DictionarySink.class.getName());
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME, "users");
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "user_id");

    testApp(BatchETLApplicationConfiguredWithArgs.class, args);
  }

  @Test
  public void testAppConfiguredByMix() throws Exception {
    Map<String, String> args = Maps.newHashMap();

    // source configuration
    args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, "userDetails");
    Schema inSchema = setSourceSchema(args);

    // transformation configuration
    setTransformationSchemaAndMapping(args, inSchema);

    // sink configuration
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME, "users");
    args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, "user_id");

    testApp(BatchETLConfiguredByMix.class, args);
  }

  @Test
  public void testAppConfiguredByCode() throws Exception {
    testApp(BatchETLConfiguredByCode.class, Collections.<String, String>emptyMap());
  }

  private void testApp(Class<? extends AbstractApplication> app, Map<String, String> args)
    throws TimeoutException, InterruptedException {

    // Simple ETL pipeline: mapreduce job that takes input from table dataset and outputs into dictionary dataset using
    //                      identity function (no actual transform)

    ApplicationManager appMngr = deployApplication(app);

    DataSetManager<Table> table = appMngr.getDataSet("userDetails");
    table.get().put(new Put("fooKey").add("userId", "1").add("firstName", "alex").add("lastName", "baranau"));
    table.flush();

    MapReduceManager mr = appMngr.startMapReduce("ETLMapReduce", args);
    mr.waitForFinish(2, TimeUnit.MINUTES);

    // verify
    DataSetManager<DictionaryDataSet> dictionary = appMngr.getDataSet(Constants.DICTIONARY_DATASET);
    // NOTE: using int value
    Assert.assertEquals("alex", Bytes.toString(dictionary.get().get("users", Bytes.toBytes(1), "first_name")));
    Assert.assertEquals("baranau", Bytes.toString(dictionary.get().get("users", Bytes.toBytes(1), "last_name")));
  }

  private Schema setSourceSchema(Map<String, String> args) {
    Schema inSchema = getInSchema();
    args.put(Constants.Batch.Source.ARG_SOURCE_SCHEMA, GSON.toJson(inSchema));
    return inSchema;
  }

  private Schema setTransformationSchemaAndMapping(Map<String, String> args, Schema inSchema) {
    args.put(Constants.Transformation.Schema.ARG_INPUT_SCHEMA, GSON.toJson(inSchema));
    Schema outSchema = getOutSchema();
    args.put(Constants.Transformation.Schema.ARG_OUTPUT_SCHEMA, GSON.toJson(outSchema));
    args.put(Constants.Transformation.SchemaMapping.ARG_MAPPING, GSON.toJson(getMapping()));

    return outSchema;
  }
}
