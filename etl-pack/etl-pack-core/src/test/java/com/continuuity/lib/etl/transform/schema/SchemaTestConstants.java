package com.continuuity.lib.etl.transform.schema;

import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.FieldType;
import com.continuuity.lib.etl.schema.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public class SchemaTestConstants {

  public static Schema getInSchema() {
    return new Schema(ImmutableList.of(new Field("userId", FieldType.INT)));
  }

  public static Schema getOutSchema() {
    return new Schema(ImmutableList.of(new Field("user_id", FieldType.INT),
                                       new Field("first_name", FieldType.STRING)));
  }

  public static ImmutableMap<String, String> getMapping() {
    return ImmutableMap.of("user_id", "userId",
                           "first_name", "lookup('users', 'userId', 'firstName')");
  }

}
