package com.continuuity.lib.etl.hive;

import com.continuuity.lib.etl.schema.FieldType;
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
