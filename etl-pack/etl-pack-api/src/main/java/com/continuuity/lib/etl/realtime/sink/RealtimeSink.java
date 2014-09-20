package com.continuuity.lib.etl.realtime.sink;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.lib.etl.ConfigurableProgram;
import com.continuuity.lib.etl.Record;

/**
 * Defines the sink for MapReduce program output
 */
public interface RealtimeSink extends ConfigurableProgram<FlowletContext> {
  void write(Record value) throws Exception;
}
