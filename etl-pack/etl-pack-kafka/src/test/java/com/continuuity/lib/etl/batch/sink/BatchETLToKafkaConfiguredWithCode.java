package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.dataset.table.Table;
import com.continuuity.lib.etl.batch.BatchETL;
import com.continuuity.lib.etl.batch.source.TableSource;
import com.continuuity.lib.etl.transform.schema.DefaultSchemaMapping;

import static com.continuuity.lib.etl.TestConstants.getInSchema;
import static com.continuuity.lib.etl.TestConstants.getMapping;
import static com.continuuity.lib.etl.TestConstants.getOutSchema;

public class BatchETLToKafkaConfiguredWithCode extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    createDataset("userDetails2", Table.class);
    configurer.setSource(new TableSource(getInSchema(), "userDetails2"));
    configurer.setTransformation(new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    // NOTE: we have to do this trick as we cannot access static variables since this class is loaded by another
    //       classloader that doesn't see those vars.
    String zkConnectionStr = System.getProperty("zk.connection.str");
    configurer.setSink(new KafkaSink(zkConnectionStr, "topic2", "user_id"));
  }
}
