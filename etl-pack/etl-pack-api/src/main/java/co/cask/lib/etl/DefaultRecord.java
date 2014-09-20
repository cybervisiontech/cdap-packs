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

package co.cask.lib.etl;

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
