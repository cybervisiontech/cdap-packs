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

package co.cask.lib.etl.transform.schema;

import co.cask.cdap.api.RuntimeContext;
import co.cask.lib.etl.AbstractConfigurableProgram;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.Programs;
import co.cask.lib.etl.Record;
import co.cask.lib.etl.schema.Schema;
import co.cask.lib.etl.transform.Transformation;
import com.google.common.collect.Maps;
import com.google.gson.Gson;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base class for transformations that involve input/output schema
 */
public abstract class SchemaTransformation
  extends AbstractConfigurableProgram<RuntimeContext> implements Transformation {

  private static final Gson GSON = new Gson();

  private Schema inputSchema = null;
  private Schema outputSchema = null;

  protected SchemaTransformation() {
  }

  protected SchemaTransformation(Schema inputSchema, Schema outputSchema) {
    this.inputSchema = inputSchema;
    this.outputSchema = outputSchema;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> args = Maps.newHashMap(super.getConfiguration());

    if (inputSchema != null) {
      args.put(Constants.Transformation.Schema.ARG_INPUT_SCHEMA, GSON.toJson(inputSchema));
    }

    if (outputSchema != null) {
      args.put(Constants.Transformation.Schema.ARG_OUTPUT_SCHEMA, GSON.toJson(outputSchema));
    }

    return args;
  }

  @Override
  public void initialize(RuntimeContext context) throws Exception {
    String is = Programs.getRequiredArgOrProperty(context, Constants.Transformation.Schema.ARG_INPUT_SCHEMA);
    inputSchema = GSON.fromJson(is, Schema.class);

    String os = Programs.getRequiredArgOrProperty(context, Constants.Transformation.Schema.ARG_OUTPUT_SCHEMA);
    outputSchema = GSON.fromJson(os, Schema.class);
  }

  @Nullable
  @Override
  public Record transform(Record input) throws IOException, InterruptedException {
    return transform(input, inputSchema, outputSchema);
  }

  protected abstract Record transform(Record input, Schema inputSchema,
                                      Schema outputSchema) throws IOException, InterruptedException;
}
