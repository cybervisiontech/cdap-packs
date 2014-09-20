/*
 * Copyright 2014 Cask, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.lib.etl.batch.sink;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.lib.etl.ConfigurableProgram;
import co.cask.lib.etl.Record;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Defines the sink for MapReduce program output
 */
public interface MapReduceSink extends ConfigurableProgram<MapReduceContext> {
  void prepareJob(MapReduceContext context) throws IOException;

  void write(Mapper.Context context, Record value) throws IOException, InterruptedException;
}
