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

package co.cask.lib.etl.schema;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 *
 */
public class FieldTypeTest {

  /**
   STRING,
   INT,
   LONG,
   DOUBLE,
   FLOAT;
   */

  @Test
  public void testFromDouble() {
    Assert.assertEquals((double) 123.03f, FieldType.DOUBLE.cast(123.03f));
    Assert.assertEquals(123.03, FieldType.DOUBLE.cast(123.03));
    Assert.assertEquals(123.03f, FieldType.FLOAT.cast(123.03f));
    Assert.assertEquals(123.03f, FieldType.FLOAT.cast(123.03));
  }

  @Test
  public void testSuspiciousCast() {
    try {
      String cast = FieldType.FLOAT.cast(123.03f);
      Assert.fail("Expected ClassCastException");
    } catch (ClassCastException e) {
      Assert.assertEquals("java.lang.Float cannot be cast to java.lang.String", e.getMessage());
    }
  }

  @Test
  public void testToFromBytes() {
    Object[][] testCases = new Object[][] {
      { "123", FieldType.STRING },
      { 123, FieldType.INT },
      { 123L, FieldType.LONG },
      { 123.0, FieldType.DOUBLE },
      { 123.0f, FieldType.FLOAT }
    };

    for (Object[] testCase : testCases) {
      Object input = testCase[0];
      FieldType type = (FieldType) testCase[1];

      Assert.assertEquals(input, type.fromBytes(type.toBytes(input)));
    }
  }

  @Test
  public void testCastString() {
    FieldType type = FieldType.STRING;
    Assert.assertEquals("123", type.cast("123"));
    Assert.assertEquals("123.0", type.cast(123.0));
    Assert.assertEquals("123.0", type.cast(123.0f));
    Assert.assertEquals("123", type.cast(123L));
    Assert.assertEquals("123", type.cast((short) 123));
    Assert.assertEquals("2", type.cast((byte) 2));
  }

  @Test
  public void testCastInt() {
    FieldType type = FieldType.INT;
    Assert.assertEquals(123, type.cast("123"));
    Assert.assertEquals(123, type.cast(123.0));
    Assert.assertEquals(123, type.cast(123.0f));
    Assert.assertEquals(123, type.cast(123L));
    Assert.assertEquals(123, type.cast((short) 123));
    Assert.assertEquals(2, type.cast((byte) 2));
  }

  @Test
  public void testCastLong() {
    FieldType type = FieldType.LONG;
    Assert.assertEquals(123L, type.cast("123"));
    Assert.assertEquals(123L, type.cast(123.0));
    Assert.assertEquals(123L, type.cast(123.0f));
    Assert.assertEquals(123L, type.cast(123L));
    Assert.assertEquals(123L, type.cast((short) 123));
    Assert.assertEquals(2L, type.cast((byte) 2));
  }

  @Test
  public void testCastDouble() {
    FieldType type = FieldType.DOUBLE;
    Assert.assertEquals(123.0, type.cast("123"));
    Assert.assertEquals(123.0, type.cast(123.0));
    Assert.assertEquals(123.0, type.cast(123.0f));
    Assert.assertEquals(123.0, type.cast(123L));
    Assert.assertEquals(123.0, type.cast((short) 123));
    Assert.assertEquals(2.0, type.cast((byte) 2));
  }

  @Test
  public void testCastFloat() {
    FieldType type = FieldType.FLOAT;
    Assert.assertEquals(123.0f, type.cast("123"));
    Assert.assertEquals(123.0f, type.cast(123.0));
    Assert.assertEquals(123.0f, type.cast(123.0f));
    Assert.assertEquals(123.0f, type.cast(123L));
    Assert.assertEquals(123.0f, type.cast((short) 123));
    Assert.assertEquals(2.0f, type.cast((byte) 2));
  }

  @Test
  public void testCastNull() {
    for (FieldType fieldType : FieldType.values()) {
      Assert.assertEquals(null, fieldType.cast(null));
    }
  }

  @Test
  public void testCastInvalid() {
    Random testInput = new Random();

    for (FieldType fieldType : FieldType.values()) {
      try {
        fieldType.cast(testInput);
        Assert.fail();
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(testInput.getClass().getName() + " cannot be casted to type "
                              + fieldType.name(), e.getMessage());
      } catch (Throwable t) {
        Assert.fail(t.getMessage());
      }
    }
  }

}
