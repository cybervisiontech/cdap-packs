package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.batch.BatchETL;
import com.continuuity.lib.etl.batch.sink.HBaseSink;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

import static com.continuuity.lib.etl.TestConstants.getInSchema;
import static com.continuuity.lib.etl.TestConstants.getMapping;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

public final class BatchETLToHBaseConfiguredWithArgs extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    createDataset("userDetails3", Table.class);
    configurer.setSource(new TableSource(getInSchema(), "userDetails3"));
    configurer.setTransformation(new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    configurer.setSink(new HBaseSink());
  }
}
