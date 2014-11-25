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

import co.cask.cdap.packs.etl.schema.Field;
import co.cask.cdap.packs.etl.schema.FieldType;
import co.cask.cdap.packs.etl.schema.Schema;
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
