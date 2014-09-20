package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.batch.BatchETL;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

import static com.continuuity.lib.etl.TestConstants.getInSchema;
import static com.continuuity.lib.etl.TestConstants.getMapping;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

public final class BatchETLToDictionaryConfiguredWithCode extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    createDataset("userDetails2", Table.class);
    configurer.setSource(new TableSource(getInSchema(), "userDetails2"));
    configurer.setTransformation(new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    configurer.setSink(new DictionarySink("users2", "user_id"));
  }
}
