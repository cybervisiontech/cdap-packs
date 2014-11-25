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

package co.cask.cdap.packs.etl.transform.script;

import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.packs.etl.schema.FieldType;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import sun.org.mozilla.javascript.internal.FunctionObject;
import sun.org.mozilla.javascript.internal.ScriptableObject;

/**
 * Used to provide functions in the {@link javax.script.ScriptContext}
 * of {@link co.cask.cdap.packs.etl.transform.schema.ScriptableSchemaMapping}.
 *
 * Provides functions (lookup, lookupInt, etc.) to query the provided
 * {@link co.cask.cdap.packs.etl.dictionary.DictionaryDataSet}.
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
