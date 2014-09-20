package com.continuuity.lib.etl.batch.source;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.schema.Schema;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.Iterator;
import java.util.Map;

/**
 * Source with schema
 */
public abstract class SchemaSource<KEY_TYPE, VALUE_TYPE> extends AbstractMapReduceSource<KEY_TYPE, VALUE_TYPE> {

  private static final Gson GSON = new Gson();

  private Schema schema = null;

  protected SchemaSource() {
  }

  protected SchemaSource(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (schema != null) {
      args.put(Constants.Batch.Source.ARG_SOURCE_SCHEMA, GSON.toJson(schema));
    }

    return args;
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    Programs.checkArgOrPropertyIsSet(context, Constants.Batch.Source.ARG_SOURCE_SCHEMA);
  }

  @Override
  public void initialize(MapReduceContext context) {
    this.schema = GSON.fromJson(Programs.getArgOrProperty(context, Constants.Batch.Source.ARG_SOURCE_SCHEMA), Schema.class);
  }

  @Override
  public Iterator<Record> read(KEY_TYPE key, VALUE_TYPE value) {
    return read(key, value, schema);
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  protected abstract Iterator<Record> read(KEY_TYPE key, VALUE_TYPE value, Schema schema);
}
