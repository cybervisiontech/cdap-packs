package com.continuuity.lib.etl.transform;

import com.continuuity.api.RuntimeContext;
import com.continuuity.lib.etl.AbstractConfigurableProgram;
import com.continuuity.lib.etl.Record;

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
