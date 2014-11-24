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

import co.cask.lib.etl.schema.FieldType;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

/**
 * Converter to hive types
 */
public enum HiveFieldType {

  STRING,
  BIGINT,
  INT,
  FLOAT,
  DOUBLE;

  public static HiveFieldType fromFieldType(FieldType type) {
    switch (type) {
      case STRING: return STRING;
      case INT: return INT;
      case LONG: return BIGINT;
      case FLOAT: return FLOAT;
      case DOUBLE: return DOUBLE;
      default: throw new IllegalArgumentException("Unknown type: " + type);
    }
  }

  public String getTypeString() {
    return this.name();
  }

  public HCatFieldSchema.Type getType() {
    switch (this) {
      case STRING: return HCatFieldSchema.Type.STRING;
      case INT: return HCatFieldSchema.Type.INT;
      case BIGINT: return HCatFieldSchema.Type.BIGINT;
      case FLOAT: return HCatFieldSchema.Type.FLOAT;
      case DOUBLE: return HCatFieldSchema.Type.DOUBLE;
      default: throw new IllegalArgumentException("Unknown type: " + this);
    }
  }
}
