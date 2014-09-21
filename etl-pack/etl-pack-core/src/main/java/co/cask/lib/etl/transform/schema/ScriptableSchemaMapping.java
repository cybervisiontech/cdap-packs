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

package co.cask.lib.etl.transform.schema;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.data.DataSetContext;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.Record;
import co.cask.lib.etl.dictionary.DictionaryDataSet;
import co.cask.lib.etl.schema.Schema;
import co.cask.lib.etl.transform.script.LookupFunction;
import co.cask.lib.etl.transform.script.ScriptBasedTransformer;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import javax.script.ScriptException;

/**
 * Transformation that supports javascript functions and dictionary lookup.
 */
public class ScriptableSchemaMapping extends SchemaMapping {
  private static final Logger LOG = LoggerFactory.getLogger(ScriptableSchemaMapping.class);
  private static final Gson GSON = new Gson();

  private DictionaryDataSet dictionaryDataSet;

  public ScriptableSchemaMapping() {
  }

  public ScriptableSchemaMapping(Schema inputSchema, Schema outputSchema, Map<String, String> mapping) {
    super(inputSchema, outputSchema, mapping);
  }

  @Override
  public void initialize(RuntimeContext context) throws Exception {
    super.initialize(context);
    // todo: this is ugly: we know that it is either MapReduce or Flowlet context, but better not to cast like that...
    dictionaryDataSet = ((DataSetContext) context).getDataSet(Constants.DICTIONARY_DATASET);
  }

  @Override
  protected Record transform(Record input, Schema inputSchema, Schema outputSchema,
                             Map<String, String> mapping) throws IOException, InterruptedException {
    // NOTE: mapping defines "outputField->outputFieldValue" mapping where outputFieldValue is a JavaScript expression
    LookupFunction dictionaryLookup = new LookupFunction(dictionaryDataSet, input);

    ScriptBasedTransformer dataProcessor = new ScriptBasedTransformer();
    try {
      return dataProcessor.transform(input, inputSchema, outputSchema, mapping,
                                     dictionaryLookup.getContextVariables());
    } catch (ScriptException e) {
      LOG.error("Error parsing expression in mapping: {}", GSON.toJson(mapping), e);
      return null;
    }
  }
}
