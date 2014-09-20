package com.continuuity.lib.etl.batch.sink;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.lib.etl.ConfigurableProgram;
import com.continuuity.lib.etl.Record;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Defines the sink for MapReduce program output
 */
public interface MapReduceSink extends ConfigurableProgram<MapReduceContext> {
  void prepareJob(MapReduceContext context) throws IOException;

  void write(Mapper.Context context, Record value) throws IOException, InterruptedException;
}
