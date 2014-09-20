package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.lib.etl.AbstractConfigurableProgram;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;

/**
 * Outputs data into named dictionary
 */
public class DictionarySink
  extends AbstractConfigurableProgram<FlowletContext> implements RealtimeSink {

  private DictionaryDataSet dictionary;

  private String dictionaryName = null;
  private String keyField = null;

  public DictionarySink() {
  }

  public DictionarySink(String dictionaryName, String keyField) {
    this.dictionaryName = dictionaryName;
    this.keyField = keyField;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());
    if (dictionaryName != null) {
      args.put(Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_NAME, dictionaryName);
    }
    if (keyField != null) {
      args.put(Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, keyField);
    }

    return args;
  }

  @Override
  public void initialize(FlowletContext context) {
    Programs.checkArgOrPropertyIsSet(context, Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_NAME);
    Programs.checkArgOrPropertyIsSet(context, Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD);
    dictionaryName = Programs.getArgOrProperty(context, Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_NAME);
    keyField = Programs.getArgOrProperty(context, Constants.Realtime.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD);
    dictionary = context.getDataSet(Constants.DICTIONARY_DATASET);
  }

  @Override
  public void write(Record record) throws IOException, InterruptedException {
    byte[] key = record.getValue(keyField);
    Map<String, byte[]> fields = Maps.newHashMap();
    for (String field : record.getFields()) {
      fields.put(field, record.getValue(field));
    }
    dictionary.write(dictionaryName, key, fields);
  }
}
