package com.continuuity.lib.etl.batch;

import com.continuuity.api.dataset.table.Table;

// Everything configured by args
public final class BatchETLApplicationConfiguredWithArgs extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    createDataset("userDetails", Table.class);
  }
}
