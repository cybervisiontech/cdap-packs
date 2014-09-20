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

package co.cask.lib.etl.batch;

import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Table;
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
