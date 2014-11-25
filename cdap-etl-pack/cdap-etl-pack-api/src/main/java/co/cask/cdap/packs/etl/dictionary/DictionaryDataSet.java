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

package co.cask.cdap.packs.etl.dictionary;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.api.dataset.DatasetSpecification;
import co.cask.cdap.api.dataset.lib.AbstractDataset;
import co.cask.cdap.api.dataset.module.EmbeddedDataset;
import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Put;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import com.google.common.base.Preconditions;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Holds named dictionaries.
 */
public class DictionaryDataSet extends AbstractDataset
  implements BatchWritable<String, DictionaryDataSet.DictionaryEntry> {

  // the underlying table
  private Table table;

  public DictionaryDataSet(DatasetSpecification spec, @EmbeddedDataset("data") Table table) {
    super(spec.getName(), table);
    this.table = table;
  }

  @Nullable
  public String get(String dictionaryName, String key, String field) {
    return Bytes.toString(get(dictionaryName, Bytes.toBytes(key), field));
  }

  @Nullable
  public byte[] get(String dictionaryName, byte[] key, String field) {
    // NOTE: doing get for the whole row intentionally so that underlying table dataset caches it
    Get get = new Get(getKey(dictionaryName, key));
    Row row = table.get(get);
    if (row != null) {
      return row.get(field);
    }

    return null;
  }

  public void write(String dictionaryName, byte[] key, Map<String, byte[]> fields) {
    Put put = new Put(getKey(dictionaryName, key));
    for (Map.Entry<String, byte[]> field : fields.entrySet()) {
      put.add(field.getKey(), field.getValue());
    }
    this.table.put(put);
  }

  private byte[] getKey(String dictionaryName, byte[] key) {
    Preconditions.checkNotNull(dictionaryName, "dictionaryName cannot be null");
    Preconditions.checkNotNull(key, "key cannot be null");
    // NOTE: adding length to avoid conflict with other dictionaries
    return Bytes.add(Bytes.toBytes(dictionaryName.length()), Bytes.toBytes(dictionaryName), key);
  }

  @Override
  public void write(String dictionaryName, DictionaryEntry entry) {
    write(dictionaryName, entry.key, entry.fields);
  }

  public static final class DictionaryEntry {
    private final byte[] key;
    private final Map<String, byte[]> fields;

    public DictionaryEntry(byte[] key, Map<String, byte[]> fields) {
      this.key = key;
      this.fields = fields;
    }
  }
}
