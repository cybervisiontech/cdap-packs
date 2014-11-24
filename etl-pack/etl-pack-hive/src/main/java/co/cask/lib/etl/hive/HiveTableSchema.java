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

package co.cask.lib.etl.hive;

import co.cask.lib.etl.schema.Field;
import co.cask.lib.etl.schema.Schema;
import com.google.common.collect.ImmutableList;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 *
 */
public class HiveTableSchema {

  private Schema tableSchema;
  // TODO: add
//  private String[] partitionKeys;

  public HiveTableSchema(Schema tableSchema) {
    this.tableSchema = tableSchema;
  }

  public Schema getTableSchema() {
    return tableSchema;
  }

  public static HCatSchema generateHCatSchema(Schema tableSchema) throws HCatException {
    ImmutableList.Builder<HCatFieldSchema> fields = ImmutableList.builder();
    for (Field field : tableSchema.getFields()) {
      HiveFieldType hiveType = HiveFieldType.fromFieldType(field.getType());
      fields.add(new HCatFieldSchema(field.getName().toLowerCase(), hiveType.getType(), null));
    }
    return new HCatSchema(fields.build());
  }
}
