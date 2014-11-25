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

package co.cask.cdap.packs.etl.schema;

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
