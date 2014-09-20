package com.continuuity.lib.etl.transform.schema;

import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.FieldType;
import com.continuuity.lib.etl.schema.Schema;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SchemaTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testSerDe() {
    Field firstField = new Field("test1", FieldType.FLOAT);
    Field secondField = new Field("test2", FieldType.FLOAT);
    Schema schema = new Schema(firstField, secondField);

    Schema actualSchema = GSON.fromJson(GSON.toJson(schema), Schema.class);
    Assert.assertEquals(firstField.getName(), actualSchema.getField("test1").getName());
    Assert.assertEquals(secondField.getName(), actualSchema.getField("test2").getName());
    Assert.assertEquals(schema.getFields().size(), actualSchema.getFields().size());
  }

}
