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

package co.cask.lib.etl.transform;

import co.cask.cdap.api.RuntimeContext;
import co.cask.lib.etl.AbstractConfigurableProgram;
import co.cask.lib.etl.Record;

import javax.annotation.Nullable;

/**
 * Transforms with identity function, i.e. output is same as input
 */
public class IdentityTransformation extends AbstractConfigurableProgram<RuntimeContext> implements Transformation {
  @Nullable
  @Override
  public Record transform(Record input) {
    return input;
  }
}
