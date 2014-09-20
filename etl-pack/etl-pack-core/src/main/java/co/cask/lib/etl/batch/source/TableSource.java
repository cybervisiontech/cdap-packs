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

package co.cask.lib.etl.batch.source;

import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.Programs;
import co.cask.lib.etl.Record;
import co.cask.lib.etl.schema.Field;
import co.cask.lib.etl.schema.Schema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * Source that provides data from {@link Table}.
 */
public class TableSource extends SchemaSource<byte[], Row> {
  private String tableName = null;

  public TableSource() {
  }

  public TableSource(Schema schema, String inputStream) {
    super(schema);
    this.tableName = inputStream;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (tableName != null) {
      args.put(Constants.Batch.Source.Table.ARG_INPUT_TABLE, tableName);
    }

    return args;
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    super.prepareJob(context);
    String tableName = Programs.getArgOrProperty(context, Constants.Batch.Source.Table.ARG_INPUT_TABLE);
    Preconditions.checkArgument(tableName != null, "Missing required argument " + Constants.Batch.Source.Table.ARG_INPUT_TABLE);
    Table table = context.getDataSet(tableName);
    context.setInput(tableName, table.getSplits());
  }

  @Override
  public Iterator<Record> read(byte[] key, Row row, Schema schema) {
    Record.Builder builder = new Record.Builder();
    for (Field f : schema.getFields()) {
      builder.add(f.getName(), row.get(f.getName()));
    }

    return Collections.singleton(builder.build()).iterator();
  }

}
