/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.lib.etl.realtime.sink;

import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.lib.etl.AbstractConfigurableProgram;
import co.cask.lib.etl.Programs;
import co.cask.lib.etl.Record;
import co.cask.lib.etl.schema.Schema;
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
