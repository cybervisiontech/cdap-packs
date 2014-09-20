package com.continuuity.lib.etl;

import com.continuuity.api.RuntimeContext;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.mapreduce.MapReduceContext;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Handy utility to work with program options, arguments, etc.
 */
public final class Programs {
  private static final Gson GSON = new Gson();

  private Programs() {}

  public static String getRequiredArgOrProperty(RuntimeContext context, String key) {
    checkArgOrPropertyIsSet(context, key);
    return getArgOrProperty(context, key);
  }

  public static String getArgOrProperty(RuntimeContext context, String key) {
    // arguments have priority, so that user can override options at run time
    String value = context.getRuntimeArguments().get(key);
    if (value != null) {
      return value;
    }
    if (context instanceof MapReduceContext) {
      return ((MapReduceContext) context).getSpecification().getProperties().get(key);
    }
    if (context instanceof FlowletContext) {
      return ((FlowletContext) context).getSpecification().getProperties().get(key);
    }
    return null;
  }

  public static String getArgOrProperty(RuntimeContext context, String key, String defaultValue) {
    String value = getArgOrProperty(context, key);
    if (value != null) {
      return value;
    } else {
      return defaultValue;
    }
  }

  public static <T> T getJsonArgOrProperty(RuntimeContext context, String key, Class<T> cls) {
    try {
      return GSON.fromJson(getArgOrProperty(context, key), cls);
    } catch (JsonSyntaxException e) {
      return null;
    }
  }

  public static void checkArgOrPropertyIsSet(RuntimeContext context, String key) {
    Preconditions.checkArgument(getArgOrProperty(context, key) != null, "Missing required argument " + key);
  }

}
