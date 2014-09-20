package com.continuuity.lib.etl.transform.script;

import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.schema.FieldType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import sun.org.mozilla.javascript.internal.FunctionObject;
import sun.org.mozilla.javascript.internal.ScriptableObject;

/**
 * Used to provide functions in the {@link javax.script.ScriptContext}
 * of {@link com.continuuity.lib.etl.transform.schema.ScriptableSchemaMapping}.
 *
 * Provides functions (lookup, lookupInt, etc.) to query the provided
 * {@link com.continuuity.lib.etl.dictionary.DictionaryDataSet}.
 */
public class LookupFunction extends ScriptableObject {

  private final DictionaryDataSet table;
  private final Record record;

  public LookupFunction(DictionaryDataSet table, Record record) {
    this.table = table;
    this.record = record;
  }

  public Object lookup(String dictionaryName, String keyFieldName, String field, String fieldTypeString) {
    Preconditions.checkArgument(dictionaryName != null, "dictionaryName must not be null");
    Preconditions.checkArgument(keyFieldName != null, "keyFieldName must not be null");
    Preconditions.checkArgument(field != null, "field must not be null");
    Preconditions.checkArgument(fieldTypeString != null, "fieldTypeString must not be null");

    if (fieldTypeString.equals("undefined")) {
      return lookup(dictionaryName, keyFieldName, field, FieldType.STRING);
    }

    return lookup(dictionaryName, keyFieldName, field, FieldType.valueOf(fieldTypeString));
  }

  private <T> T lookup(String dictionaryName, String keyFieldName, String field, FieldType fieldType) {
    return fieldType.fromBytes(lookupBytes(dictionaryName, keyFieldName, field));
  }

  private byte[] lookupBytes(String dictionaryName, String keyFieldName, String field) {
    byte[] keyBytes = record.getValue(keyFieldName);
    return table.get(dictionaryName, keyBytes, field);
  }

  @Override
  public String getClassName() {
    return LookupFunction.class.getName();
  }

  public ContextVariable[] getContextVariables() {
    try {
      return new ContextVariable[] {
        new ContextVariable("lookup", new FunctionObject(
          "lookup", getClass().getDeclaredMethod(
          "lookup", String.class, String.class, String.class, String.class), this))
      };
    } catch (NoSuchMethodException e) {
      Throwables.propagate(e);
      return null;
    }
  }
}
