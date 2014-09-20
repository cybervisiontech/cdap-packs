package com.continuuity.att.dictionaryproc;

import com.continuuity.lib.etl.schema.FieldType;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DictionaryOpsTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testSerializeFieldType() {
    FieldType fieldType = GSON.fromJson("INT", FieldType.class);
    Assert.assertEquals(FieldType.INT, fieldType);
  }

}
