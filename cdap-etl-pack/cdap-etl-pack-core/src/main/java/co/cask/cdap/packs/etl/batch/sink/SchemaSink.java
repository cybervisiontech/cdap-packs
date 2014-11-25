/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.packs.etl.batch.sink;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.packs.etl.AbstractConfigurableProgram;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.schema.Schema;
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
