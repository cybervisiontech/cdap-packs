package com.continuuity.lib.etl.batch.source;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.lib.etl.AbstractConfigurableProgram;

/**
 * Provides "do nothing" implementation for most methods of {@link com.continuuity.lib.etl.batch.source.MapReduceSource}
 */
public abstract class AbstractMapReduceSource<KEY_TYPE, VALUE_TYPE>
  extends AbstractConfigurableProgram<MapReduceContext> implements MapReduceSource<KEY_TYPE, VALUE_TYPE> {
  @Override
  public void prepareJob(MapReduceContext context) {
    // do nothing
  }

  @Override
  public void initialize(MapReduceContext context) throws Exception {
    // do nothing
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    // do nothing
  }

  @Override
  public void destroy() {
    // do nothing
  }
}
