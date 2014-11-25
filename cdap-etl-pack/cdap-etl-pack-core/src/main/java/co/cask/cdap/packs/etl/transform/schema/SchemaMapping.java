/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.packs.etl.transform.schema;

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.packs.etl.Constants;
import co.cask.cdap.packs.etl.Programs;
import co.cask.cdap.packs.etl.Record;
import co.cask.cdap.packs.etl.schema.Schema;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.io.IOException;
import java.util.Map;

/**
 * Base class for transformations that involve input/output schema
 */
public abstract class SchemaMapping extends SchemaTransformation {

  private static final Gson GSON = new Gson();

  private Map<String, String> mapping = null;

  protected SchemaMapping() {
  }

  protected SchemaMapping(Schema inputSchema, Schema outputSchema, Map<String, String> mapping) {
    super(inputSchema, outputSchema);
    this.mapping = mapping;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());

    if (mapping != null) {
      args.put(Constants.Transformation.SchemaMapping.ARG_MAPPING, GSON.toJson(mapping));
    }

    return args;
  }

  @Override
  public void initialize(RuntimeContext context) throws Exception {
    super.initialize(context);
    String mapping = Programs.getRequiredArgOrProperty(context, Constants.Transformation.SchemaMapping.ARG_MAPPING);
    this.mapping = GSON.fromJson(mapping, new TypeToken<Map<String, String>>() {}.getType());
  }

  @Nullable
  protected Record transform(Record input, Schema inputSchema,
                             Schema outputSchema) throws IOException, InterruptedException {
    return transform(input, inputSchema, outputSchema, mapping);
  }

  @Nullable
  abstract protected Record transform(Record input, Schema inputSchema,
                                      Schema outputSchema,
                                      Map<String, String> mapping) throws IOException, InterruptedException;
}
