package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.lib.etl.realtime.RealtimeETL;
import com.continuuity.lib.etl.realtime.sink.HBaseSink;
import com.continuuity.lib.etl.realtime.source.SchemaSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

import static com.continuuity.lib.etl.TestConstants.getInSchema;
import static com.continuuity.lib.etl.TestConstants.getMapping;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

public class RealtimeETLToHBaseConfiguredWithArgs extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    configurer.setInputStream("stream1");
    configurer.setSource(new SchemaSource(getInSchema()));
    configurer.setTransformation(new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    configurer.setSink(new HBaseSink());
  }
}
