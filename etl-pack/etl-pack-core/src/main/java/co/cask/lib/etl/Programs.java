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

package co.cask.lib.etl;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.mapreduce.MapReduceContext;
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
