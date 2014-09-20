package com.continuuity.lib.etl.schema;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Defines schema of data record.
 */
public class Schema {
  private final List<Field> fields;
  private transient Map<String, Field> fieldsMap;

  public Schema(List<Field> fields) {
    this.fields = Collections.unmodifiableList(fields);
  }

  public Schema(Field...fields) {
    this(Arrays.asList(fields));
  }

  public List<Field> getFields() {
    return fields;
  }

  public Field getField(String name) {
    return getFieldsMap().get(name);
  }

  private Map<String, Field> getFieldsMap() {
    // TODO: cleanup - not thread-safe, but probably OK for now
    if (fieldsMap == null) {
      ImmutableMap.Builder<String, Field> builder = ImmutableMap.builder();
      for (Field f : fields) {
        builder.put(f.getName(), f);
      }
      this.fieldsMap = builder.build();
    }

    return fieldsMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Schema schema = (Schema) o;

    if (fields != null ? !fields.equals(schema.fields) : schema.fields != null) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = fields != null ? fields.hashCode() : 0;
    return result;
  }
}
