package com.continuuity.lib.etl.batch;

import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.batch.sink.DictionarySink;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

import static com.continuuity.lib.etl.TestConstants.getInSchema;
import static com.continuuity.lib.etl.TestConstants.getMapping;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

// Source, transformation and sink types configured by app code, other stuff configured by arguments
public final class BatchETLConfiguredByCode extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    createDataset("userDetails", Table.class);
    configurer.setSource(new TableSource(getInSchema(), "userDetails"));
    configurer.setTransformation(new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    configurer.setSink(new DictionarySink("users", "user_id"));
  }
}
