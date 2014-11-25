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

package co.cask.cdap.packs.etl.realtime.sink;

import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.packs.etl.AbstractConfigurableProgram;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
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
