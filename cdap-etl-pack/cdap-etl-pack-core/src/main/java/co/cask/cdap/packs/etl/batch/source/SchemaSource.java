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

package co.cask.cdap.packs.etl.batch.source;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.schema.Schema;
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
