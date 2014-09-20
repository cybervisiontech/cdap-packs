package com.continuuity.lib.etl.transform.schema;

import com.continuuity.api.RuntimeContext;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.schema.Schema;
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
