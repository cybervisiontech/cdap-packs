package com.continuuity.lib.etl.transform;

import com.continuuity.api.RuntimeContext;
import com.continuuity.lib.etl.ConfigurableProgram;
import com.continuuity.lib.etl.Record;

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
