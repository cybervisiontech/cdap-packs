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

package co.cask.lib.etl.batch.source;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.lib.etl.AbstractConfigurableProgram;

/**
 * Provides "do nothing" implementation for most methods of {@link co.cask.lib.etl.batch.source.MapReduceSource}
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
