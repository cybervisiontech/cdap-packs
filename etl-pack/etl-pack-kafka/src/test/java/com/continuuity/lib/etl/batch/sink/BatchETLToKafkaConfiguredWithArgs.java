package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.batch.BatchETL;
import com.google.common.base.Preconditions;

// Everything configured by args
public final class BatchETLToKafkaConfiguredWithArgs extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    // hack to make reactor test framework package KafkaSink into the unit test app jar
    Preconditions.checkNotNull(KafkaSink.class);
    createDataset("userDetails1", Table.class);
  }
}
