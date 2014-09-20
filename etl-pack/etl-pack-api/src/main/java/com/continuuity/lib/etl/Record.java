package com.continuuity.lib.etl;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Map;

/**
 * Data record.
 */
public interface Record {
  /**
   * @param fieldName name of the field to retrieve
   * @return value of the field
   */
  byte[] getValue(String fieldName);

  /**
   * @return list of all field names of this record
   */
  Collection<String> getFields();

  public static class Builder {
    private final Map<String, byte[]> fields = Maps.newHashMap();

    public Builder add(String fieldName, byte[] value) {
      fields.put(fieldName, value);
      return this;
    }

    public Builder remove(String fieldName) {
      fields.remove(fieldName);
      return this;
    }

    public Record build() {
      return new DefaultRecord(fields);
    }
  }
}
