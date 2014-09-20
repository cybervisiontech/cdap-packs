package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.lib.etl.realtime.RealtimeETL;
import com.continuuity.lib.etl.realtime.source.SchemaSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

import static com.continuuity.lib.etl.TestConstants.getInSchema;
import static com.continuuity.lib.etl.TestConstants.getMapping;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

public class RealtimeETLToKafkaConfiguredWithCode extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    configurer.setInputStream("stream2");
    configurer.setSource(new SchemaSource(getInSchema()));
    configurer.setTransformation(new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    // NOTE: we have to do this trick as we cannot access static variables since this class is loaded by another
    //       classloader that doesn't see those vars.
    String zkConnectionStr = System.getProperty("zk.connection.str");
    configurer.setSink(new KafkaSink(zkConnectionStr, "topic2", "user_id"));
  }
}
