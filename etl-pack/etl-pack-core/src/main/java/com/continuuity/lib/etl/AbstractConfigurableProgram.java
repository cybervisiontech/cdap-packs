package com.continuuity.lib.etl;

import com.continuuity.api.RuntimeContext;

import java.util.Collections;
import java.util.Map;

/**
 * Provides {@link AbstractConfigurableProgram} implementation that does nothing.
 */
public abstract class AbstractConfigurableProgram<T extends RuntimeContext> implements ConfigurableProgram<T> {
  @Override
  public Map<String, String> getConfiguration() {
    // nothing by default
    return Collections.emptyMap();
  }

  @Override
  public void initialize(T t) throws Exception {
    // do nothing
  }

  @Override
  public void destroy() {
    // do nothing
  }
}
