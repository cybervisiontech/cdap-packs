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

package co.cask.lib.etl.batch.sink;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.lib.etl.AbstractConfigurableProgram;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.Programs;
import co.cask.lib.etl.Record;
import co.cask.lib.etl.dictionary.DictionaryDataSet;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Outputs data into named dictionary
 */
public class DictionarySink
  extends AbstractConfigurableProgram<MapReduceContext> implements MapReduceSink {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(DictionarySink.class);

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
      args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME, dictionaryName);
    }
    if (keyField != null) {
      args.put(Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD, keyField);
    }

    return args;
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    dictionaryName = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME);
    keyField = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD);

    context.setOutput(Constants.DICTIONARY_DATASET);
  }

  @Override
  public void initialize(MapReduceContext context) {
    dictionaryName = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_NAME);
    keyField = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Dictionary.ARG_DICTIONARY_KEY_FIELD);
  }

  @Override
  public void write(Mapper.Context context, Record record) throws IOException, InterruptedException {
    byte[] key = record.getValue(keyField);
    Preconditions.checkNotNull(key, "Value for dictionary key field " + keyField + " was null."
                               + " record=" + GSON.toJson(record));

    Map<String, byte[]> fields = Maps.newHashMap();
    for (String field : record.getFields()) {
      fields.put(field, record.getValue(field));
    }
    context.write(dictionaryName, new DictionaryDataSet.DictionaryEntry(key, fields));
  }
}
