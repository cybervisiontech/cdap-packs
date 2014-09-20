package com.continuuity.lib.etl.realtime.source;

import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.lib.etl.ConfigurableProgram;
import com.continuuity.lib.etl.Record;

import java.util.Iterator;

/**
 * Defines source for MapReduce job
 */
public interface RealtimeSource extends ConfigurableProgram<FlowletContext> {
  Iterator<Record> read(StreamEvent streamEvent) throws Exception;
}
