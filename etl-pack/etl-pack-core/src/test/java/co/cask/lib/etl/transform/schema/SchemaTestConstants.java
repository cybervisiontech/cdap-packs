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

import co.cask.lib.etl.schema.Field;
import co.cask.lib.etl.schema.FieldType;
import co.cask.lib.etl.schema.Schema;
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
