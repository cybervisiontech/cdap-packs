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

package co.cask.lib.etl.realtime.source;

import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.lib.etl.AbstractConfigurableProgram;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.Programs;
import co.cask.lib.etl.Record;
import co.cask.lib.etl.schema.Field;
import co.cask.lib.etl.schema.Schema;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.util.Iterator;
import java.util.Map;

/**
 * Source with schema
 */
public class SchemaSource extends AbstractConfigurableProgram<FlowletContext> implements RealtimeSource {

  private static final Gson GSON = new Gson();

  private static final String DEFAULT_FIELD_SEPARATOR = ",";
  private static final String DEFAULT_RECORD_SEPARATOR = "\n";

  private Schema schema = null;
  private String fieldSeparator;
  private String recordSeparator;

  public SchemaSource() {
  }

  public SchemaSource(Schema schema) {
    this.schema = schema;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (schema != null) {
      args.put(Constants.Realtime.Source.ARG_SOURCE_SCHEMA, GSON.toJson(schema));
    }
    if (fieldSeparator != null) {
      args.put(Constants.Realtime.Source.Stream.ARG_FIELD_SEPARATOR, fieldSeparator);
    }
    if (recordSeparator != null) {
      args.put(Constants.Realtime.Source.Stream.ARG_RECORD_SEPARATOR, recordSeparator);
    }

    return args;
  }

  @Override
  public void initialize(FlowletContext context) {
    Programs.checkArgOrPropertyIsSet(context, Constants.Realtime.Source.ARG_SOURCE_SCHEMA);
    this.schema = GSON.fromJson(Programs.getArgOrProperty(context, Constants.Realtime.Source.ARG_SOURCE_SCHEMA), Schema.class);
    this.fieldSeparator = Programs.getArgOrProperty(context, Constants.Realtime.Source.Stream.ARG_FIELD_SEPARATOR, DEFAULT_FIELD_SEPARATOR);
    this.recordSeparator = Programs.getArgOrProperty(context, Constants.Realtime.Source.Stream.ARG_RECORD_SEPARATOR, DEFAULT_RECORD_SEPARATOR);
  }

  @Override
  public Iterator<Record> read(StreamEvent streamEvent) throws Exception {
    return read(streamEvent, schema);
  }

  protected Iterator<Record> read(StreamEvent streamEvent, Schema schema) {
    String value = Charsets.UTF_8.decode(streamEvent.getBody()).toString();

    ImmutableList.Builder<Record> records = ImmutableList.builder();
    String recordSeparator = getRecordSeparator();
    if (recordSeparator != null) {
      String data = value.toString();
      String[] recordDatas = data.split(recordSeparator);
      for (String recordData : recordDatas) {
        addRecord(records, recordData, schema);
      }
    } else {
      addRecord(records, value.toString(), schema);
    }

    return records.build().iterator();
  }

  private void addRecord(ImmutableList.Builder<Record> records, String data, Schema schema) {
    String[] values = data.split(getFieldSeparator());
    int i = 0;
    Record.Builder builder = new Record.Builder();
    for (Field f : schema.getFields()) {
      builder.add(f.getName(), f.getType().toBytes(values[i++]));
    }
    records.add(builder.build());
  }

  private String getFieldSeparator() {
    return fieldSeparator;
  }

  private String getRecordSeparator() {
    return recordSeparator;
  }
}
