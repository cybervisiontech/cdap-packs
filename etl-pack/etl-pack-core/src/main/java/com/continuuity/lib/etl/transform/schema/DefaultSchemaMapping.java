package com.continuuity.lib.etl.transform.schema;

import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.Schema;

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
