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

package co.cask.att.dictionary;

import co.cask.lib.etl.schema.FieldType;
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
