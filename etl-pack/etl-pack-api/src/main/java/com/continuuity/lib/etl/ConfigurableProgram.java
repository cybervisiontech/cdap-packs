package com.continuuity.lib.etl;

import com.continuuity.api.ProgramLifecycle;
import com.continuuity.api.RuntimeContext;

import java.util.Map;

/**
 * A program component that provides deploy-time configuration.
 */
public interface ConfigurableProgram<T extends RuntimeContext> extends ProgramLifecycle<T> {
  /**
   * Use it to provide extra arguments during configuring program that uses it
   */
  Map<String, String> getConfiguration();
}
