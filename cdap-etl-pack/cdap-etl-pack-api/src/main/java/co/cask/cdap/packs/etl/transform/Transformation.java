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

package co.cask.cdap.packs.etl.transform;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.packs.etl.ConfigurableProgram;
import co.cask.cdap.packs.etl.Record;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Defines one-by-one data records transformation logic
 */
public interface Transformation extends ConfigurableProgram<RuntimeContext> {
  /**
   * Performs transformation of a data record
   * @param input record to transform
   * @return transformation result or {@code null} if rec
   */
  @Nullable
  Record transform(Record input) throws IOException, InterruptedException;
}
