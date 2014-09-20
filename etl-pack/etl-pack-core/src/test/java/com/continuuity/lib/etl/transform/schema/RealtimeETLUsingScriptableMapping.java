package com.continuuity.lib.etl.transform.schema;

import com.continuuity.lib.etl.realtime.RealtimeETL;
import com.continuuity.lib.etl.realtime.sink.DictionarySink;
import com.continuuity.lib.etl.realtime.source.SchemaSource;

import static com.continuuity.lib.etl.transform.schema.SchemaTestConstants.getInSchema;
import static com.continuuity.lib.etl.transform.schema.SchemaTestConstants.getMapping;
import static com.continuuity.lib.etl.transform.schema.SchemaTestConstants.getOutSchema;

public class RealtimeETLUsingScriptableMapping extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    configurer.setInputStream("usersStream");
    configurer.setSource(new SchemaSource(getInSchema()));
    configurer.setTransformation(new ScriptableSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    configurer.setSink(new DictionarySink("myDict", "user_id"));
  }
}
