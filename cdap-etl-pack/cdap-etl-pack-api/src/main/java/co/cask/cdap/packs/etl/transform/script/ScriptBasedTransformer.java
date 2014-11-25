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
import co.cask.cdap.packs.etl.schema.Field;
import co.cask.cdap.packs.etl.schema.FieldType;
import co.cask.cdap.packs.etl.schema.Schema;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

/**
 * Transforms input records to output records based on a mapping.
 * The mapping is treated as "outputFieldName" -> "outputFieldExpression".
 */
// TODO: move to api module
public class ScriptBasedTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(ScriptBasedTransformer.class);

  private ScriptEngineManager factory = new ScriptEngineManager();
  private ScriptEngine engine = factory.getEngineByName("JavaScript");

  public Record transform(Record input, Schema inputSchema, Schema outputSchema,
                          Map<String, String> mapping, ContextVariable... contextVariables) throws ScriptException {
    ScriptContext scriptContext = setupContext(input, inputSchema, contextVariables);
    return processOutput(outputSchema, mapping, scriptContext);
  }

  public Record generateDefaultRecord(Schema schema) {
    Record.Builder record = new Record.Builder();

    for (Field field : schema.getFields()) {
      FieldType type = field.getType();
      record.add(field.getName(), type.toBytes(type.getDefaultValue()));
    }

    return record.build();
  }

  private ScriptContext setupContext(Record input, Schema inputSchema,
                                     ContextVariable...variables) {

    SimpleScriptContext scriptContext = new SimpleScriptContext();

    // set context variables
    for (ContextVariable variable : variables) {
      Preconditions.checkState(scriptContext.getAttribute(variable.getName(), ScriptContext.ENGINE_SCOPE) == null,
                               "Context variable " + variable.getName() + " cannot be redefined");
      scriptContext.setAttribute(variable.getName(), variable.getValue(), ScriptContext.ENGINE_SCOPE);
    }

    // set input variables
    for (Field inputColumn : inputSchema.getFields()) {
      FieldType type = inputColumn.getType();
      byte[] valueBytes = input.getValue(inputColumn.getName());
      if (valueBytes == null) {
        throw new IllegalArgumentException("No value found in input for column " + inputColumn);
      }
      Object inputValue = type.fromBytes(valueBytes);
      scriptContext.setAttribute(inputColumn.getName(), inputValue, ScriptContext.ENGINE_SCOPE);
    }

    return scriptContext;
  }

  private Record processOutput(Schema rowSchema, Map<String, String> mapping,
                                     ScriptContext scriptContext) throws ScriptException {

    Record.Builder record = new Record.Builder();

    // process and interpret
    List<Field> outputColumns = rowSchema.getFields();
    for (Field outputColumn : outputColumns) {
      FieldType type = outputColumn.getType();
      String expression = mapping.get(outputColumn.getName());
      if (expression == null) {
        record.add(outputColumn.getName(), type.toBytes(type.getDefaultValue()));
        continue;
      }

      Object processedValue = engine.eval(expression, scriptContext);
      if (processedValue == null) {
        processedValue = type.getDefaultValue();
      }
      processedValue = type.cast(processedValue);
      record.add(outputColumn.getName(), type.toBytes(processedValue));
    }

    return record.build();
  }

}
