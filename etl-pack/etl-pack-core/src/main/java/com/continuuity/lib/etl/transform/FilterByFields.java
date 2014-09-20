package com.continuuity.lib.etl.transform;

import com.continuuity.api.RuntimeContext;
import com.continuuity.lib.etl.AbstractConfigurableProgram;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
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
