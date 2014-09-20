package com.continuuity.lib.etl;

import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.FieldType;
import com.continuuity.lib.etl.schema.Schema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 *
 */
public class TestConstants {


  public static Schema getInSchema() {
    return new Schema(ImmutableList.of(new Field("userId", FieldType.STRING),
                                       new Field("firstName", FieldType.STRING),
                                       new Field("lastName", FieldType.STRING)));
  }

  public static Schema getOutSchema() {
    return new Schema(ImmutableList.of(new Field("user_id", FieldType.INT),
                                       new Field("first_name", FieldType.STRING),
                                       new Field("last_name", FieldType.STRING)));
  }

  public static ImmutableMap<String, String> getMapping() {
    return ImmutableMap.of("userId", "user_id",
                           "firstName", "first_name",
                           "lastName", "last_name");
  }

}
