package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.batch.BatchETL;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

import static com.continuuity.lib.etl.TestConstants.getInSchema;
import static com.continuuity.lib.etl.TestConstants.getMapping;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

public final class BatchETLToDictionaryConfiguredWithArgs extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    createDataset("userDetails1", Table.class);
    configurer.setSource(new TableSource(getInSchema(), "userDetails1"));
    configurer.setTransformation(new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
  }
}
