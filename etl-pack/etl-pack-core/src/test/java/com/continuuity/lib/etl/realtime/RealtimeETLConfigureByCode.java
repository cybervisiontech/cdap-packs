package com.continuuity.lib.etl.realtime;

import com.continuuity.lib.etl.TestConstants;
import com.continuuity.lib.etl.realtime.sink.DictionarySink;
import com.continuuity.lib.etl.realtime.source.SchemaSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

// Source, transformation and sink types configured by app code, other stuff configured by arguments
public class RealtimeETLConfigureByCode extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    configurer.setInputStream("userDetailsStream");
    configurer.setSource(new SchemaSource(TestConstants.getInSchema()));
    configurer.setTransformation(new DefaultSchemaMapping(TestConstants.getInSchema(),
                                                          TestConstants.getOutSchema(),
                                                          TestConstants.getMapping()));
    configurer.setSink(new DictionarySink("users", "user_id"));
  }
}
