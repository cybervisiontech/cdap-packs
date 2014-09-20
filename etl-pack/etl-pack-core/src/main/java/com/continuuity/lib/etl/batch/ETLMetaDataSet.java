package com.continuuity.lib.etl.batch;

import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Table;
import com.google.gson.Gson;

import javax.annotation.Nullable;

/**
 * Holds meta information about ETL tasks.
 */
public class ETLMetaDataSet extends AbstractDataset {
  private static final Gson GSON = new Gson();
  private static final String COLUMN = "c";

  // the underlying table
  private Table table;

  public ETLMetaDataSet(DatasetSpecification spec, @EmbeddedDataset("data") Table table) {
    super(spec.getName(), table);
    this.table = table;
  }

  @Nullable
  public <T> T get(String taskId, Class<T> clazz) {
    String meta = table.get(new Get(taskId, COLUMN)).getString(COLUMN);
    if (meta == null) {
      return null;
    }

    return GSON.fromJson(meta, clazz);
  }

  public <T> void set(String taskId, T meta) {
    this.table.put(new Put(taskId, COLUMN, GSON.toJson(meta)));
  }
}
