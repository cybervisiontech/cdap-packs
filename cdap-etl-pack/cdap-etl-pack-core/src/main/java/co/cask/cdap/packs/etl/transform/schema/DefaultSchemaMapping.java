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

package co.cask.cdap.packs.etl.transform.schema;

import co.cask.cdap.packs.etl.etl.Record;
import co.cask.cdap.packs.etl.schema.Field;
import co.cask.cdap.packs.etl.schema.Schema;

import java.util.Map;

/**
 * Does simple transformation using fields mapping.
 */
public class DefaultSchemaMapping extends SchemaMapping {
  public DefaultSchemaMapping() {
  }

  public DefaultSchemaMapping(Schema inputSchema, Schema outputSchema, Map<String, String> mapping) {
    super(inputSchema, outputSchema, mapping);
  }

  @Override
  protected Record transform(Record input, Schema inputSchema, Schema outputSchema, Map<String, String> mapping) {
    // NOTE: mapping defines "inputField->outputField" mapping
    Record.Builder builder = new Record.Builder();
    for (Map.Entry<String, String> rule : mapping.entrySet()) {
      Field inField = inputSchema.getField(rule.getKey());
      Field outField = outputSchema.getField(rule.getValue());
      Object inValue = inField.getType().fromBytes(input.getValue(inField.getName()));
      builder.add(outField.getName(), outField.getType().toBytes(inValue));
    }
    return builder.build();
  }
}
