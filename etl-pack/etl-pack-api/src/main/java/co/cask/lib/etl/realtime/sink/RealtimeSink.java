/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.lib.etl.realtime.sink;

import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.lib.etl.ConfigurableProgram;
import co.cask.lib.etl.Record;

/**
 * Defines the sink for MapReduce program output
 */
public interface RealtimeSink extends ConfigurableProgram<FlowletContext> {
  void write(Record value) throws Exception;
}
