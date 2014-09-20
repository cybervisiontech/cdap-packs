package com.continuuity.lib.etl.batch.source;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.lib.etl.ConfigurableProgram;
import com.continuuity.lib.etl.Record;

import java.util.Iterator;

/**
 * Defines source for MapReduce job
 */
public interface MapReduceSource<KEY_TYPE, VALUE_TYPE> extends ConfigurableProgram<MapReduceContext> {
  void prepareJob(MapReduceContext context);

  void onFinish(boolean succeeded, MapReduceContext context) throws Exception;

  Iterator<Record> read(KEY_TYPE key, VALUE_TYPE value);
}
