package com.continuuity.lib.etl.batch;

import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.batch.sink.DictionarySink;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

// Source, transformation and sink types configured by app code, other stuff configured by arguments
public final class BatchETLConfiguredByMix extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    createDataset("userDetails", Table.class);
    configurer.setSource(new TableSource());
    configurer.setTransformation(new DefaultSchemaMapping());
    configurer.setSink(new DictionarySink());
  }
}
