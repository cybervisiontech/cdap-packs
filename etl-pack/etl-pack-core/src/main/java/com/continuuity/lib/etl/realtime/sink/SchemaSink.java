package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.lib.etl.AbstractConfigurableProgram;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.schema.Schema;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.Map;

/**
 * Sink that has schema associated with it
 */
public abstract class SchemaSink
  extends AbstractConfigurableProgram<FlowletContext> implements RealtimeSink {

  public static final String ARG_SCHEMA = "etl.sink.realtime.schema";

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
      args.put(ARG_SCHEMA, GSON.toJson(schema));
    }

    return args;
  }

  @Override
  public void initialize(FlowletContext context) throws IOException {
    Programs.checkArgOrPropertyIsSet(context, ARG_SCHEMA);
    this.schema = GSON.fromJson(Programs.getArgOrProperty(context, ARG_SCHEMA), Schema.class);
  }

  @Override
  public void write(Record value) throws Exception {
    write(value, schema);
  }

  protected abstract void write(Record value, Schema schema) throws Exception;
}
