package com.continuuity.lib.etl.transform.script;

/**
 * Context variable to be made available to be used in expressions for ScriptBasedTransformer.transform().
 */
public class ContextVariable {

  private String name;
  private Object value;

  public ContextVariable(String name, Object value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public Object getValue() {
    return value;
  }
}
