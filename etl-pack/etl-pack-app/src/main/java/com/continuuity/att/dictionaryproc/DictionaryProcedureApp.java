package com.continuuity.att.dictionaryproc;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;

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
