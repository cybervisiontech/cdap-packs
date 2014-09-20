package com.continuuity.lib.etl;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of {@link Record}
 */
class DefaultRecord implements Record {
  private final Map<String, byte[]> fields;

  public DefaultRecord(Map<String, byte[]> fields) {
    this.fields = fields;
  }

  @SuppressWarnings("unchecked")
  @Override
  public byte[] getValue(String fieldName) {
    return fields.get(fieldName);
  }

  @Override
  public Collection<String> getFields() {
    return Collections.unmodifiableSet(fields.keySet());
  }
}
