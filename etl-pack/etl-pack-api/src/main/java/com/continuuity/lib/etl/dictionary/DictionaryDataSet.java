package com.continuuity.lib.etl.dictionary;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.batch.BatchWritable;
import com.continuuity.api.dataset.DatasetSpecification;
import com.continuuity.api.dataset.lib.AbstractDataset;
import com.continuuity.api.dataset.module.EmbeddedDataset;
import com.continuuity.api.dataset.table.Get;
import com.continuuity.api.dataset.table.Put;
import com.continuuity.api.dataset.table.Row;
import com.continuuity.api.dataset.table.Table;
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
