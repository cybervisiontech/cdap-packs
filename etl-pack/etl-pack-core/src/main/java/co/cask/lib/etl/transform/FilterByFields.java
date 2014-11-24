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
import co.cask.lib.etl.Programs;
import co.cask.lib.etl.Record;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Filters records by fields and their values
 */
public class FilterByFields extends AbstractConfigurableProgram<RuntimeContext> implements Transformation {
  public static final String ARG_INCLUDE_BY = "etl.transform.filterByFields.includeBy";

  private static final Gson GSON = new Gson();

  private Map<String, String> includeBy;

  @Override
  public void initialize(RuntimeContext context) {
    String includeBy = Programs.getArgOrProperty(context, ARG_INCLUDE_BY);
    Preconditions.checkArgument(includeBy != null, "Missing required argument " + ARG_INCLUDE_BY);
    this.includeBy = GSON.fromJson(includeBy, new TypeToken<Map<String, String>>() {}.getType());
  }

  @Nullable
  @Override
  public Record transform(Record input) {
    for (Map.Entry<String, String> mustHave : includeBy.entrySet()) {
      if (!mustHave.getValue().equals(input.getValue(mustHave.getKey()))) {
        return null;
      }
    }
    return input;
  }
}
