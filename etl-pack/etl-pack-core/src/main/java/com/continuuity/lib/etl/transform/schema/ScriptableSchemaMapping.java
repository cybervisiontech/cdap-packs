package com.continuuity.lib.etl.transform.schema;

import com.continuuity.api.RuntimeContext;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.lib.etl.transform.script.LookupFunction;
import com.continuuity.lib.etl.transform.script.ScriptBasedTransformer;
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
    dictionaryDataSet = context.getDataSet(Constants.DICTIONARY_DATASET);
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
