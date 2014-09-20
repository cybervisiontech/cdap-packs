package com.continuuity.lib.etl.hive;

import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.Schema;
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
