package com.continuuity.lib.etl.transform.schema;

import com.continuuity.api.RuntimeContext;
import com.continuuity.lib.etl.AbstractConfigurableProgram;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.lib.etl.transform.Transformation;
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
