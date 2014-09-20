package com.continuuity.lib.etl.batch.source;

import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.Schema;
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
