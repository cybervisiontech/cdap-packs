package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.lib.etl.realtime.RealtimeETL;
import com.continuuity.lib.etl.realtime.source.MetadataSource;
import com.continuuity.lib.etl.transform.IdentityTransformation;

public class RealtimeETLToDictionaryConfiguredWithArgs extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    configurer.setInputStream("filesStream");
    configurer.setSource(new MetadataSource());
    configurer.setTransformation(new IdentityTransformation());
    configurer.setSink(new DictionarySink());
  }
}