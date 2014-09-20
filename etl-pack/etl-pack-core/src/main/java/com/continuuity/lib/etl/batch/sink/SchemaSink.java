package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.lib.etl.AbstractConfigurableProgram;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.schema.Schema;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

/**
 * Sink that has schema associated with it
 */
public abstract class SchemaSink
  extends AbstractConfigurableProgram<MapReduceContext> implements MapReduceSink {

  private static final Gson GSON = new Gson();

  private Schema schema = null;

  public SchemaSink() {
  }

  protected SchemaSink(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (schema != null) {
      args.put(Constants.Batch.Sink.ARG_SINK_SCHEMA, GSON.toJson(schema));
    }

    return args;
  }

  @Override
  public void prepareJob(MapReduceContext context) throws IOException {
    Programs.checkArgOrPropertyIsSet(context, Constants.Batch.Sink.ARG_SINK_SCHEMA);
    this.schema = GSON.fromJson(Programs.getArgOrProperty(context, Constants.Batch.Sink.ARG_SINK_SCHEMA), Schema.class);
  }

  @Override
  public void initialize(MapReduceContext context) throws IOException {
    this.schema = GSON.fromJson(Programs.getArgOrProperty(context, Constants.Batch.Sink.ARG_SINK_SCHEMA), Schema.class);
  }

  @Override
  public void write(Mapper.Context context, Record value) throws IOException, InterruptedException {
    write(context, value, schema);
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  protected abstract void write(Mapper.Context context, Record value, Schema schema)
    throws IOException, InterruptedException;
}
