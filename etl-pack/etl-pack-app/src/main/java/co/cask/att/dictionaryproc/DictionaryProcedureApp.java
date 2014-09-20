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

package co.cask.att.dictionaryproc;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.dictionary.DictionaryDataSet;

/**
 * StaticFeedApp analyzes Apache access log data and aggregates
 * the number of HTTP requests each hour over the last 24 hours.
 */
public class DictionaryProcedureApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("DictionaryProcedureApp");
    setDescription("Query the dictionary dataset");
    createDataset(Constants.DICTIONARY_DATASET, DictionaryDataSet.class);
    addProcedure(new DictionaryOps());
  }

}
