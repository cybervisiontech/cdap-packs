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

package co.cask.lib.etl;

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
