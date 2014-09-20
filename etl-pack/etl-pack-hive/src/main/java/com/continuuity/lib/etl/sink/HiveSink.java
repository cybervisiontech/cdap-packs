package com.continuuity.lib.etl.sink;

import com.continuuity.api.mapreduce.MapReduceContext;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.batch.sink.MapReduceSink;
import com.continuuity.lib.etl.batch.sink.SchemaSink;
import com.continuuity.lib.etl.hive.HiveFieldType;
import com.continuuity.lib.etl.hive.HiveTableSchema;
import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.FieldType;
import com.continuuity.lib.etl.schema.Schema;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Outputs records into a Hive table.
 */
public class HiveSink extends SchemaSink implements MapReduceSink {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSink.class);

  private static final Gson GSON = new Gson();

  private String hiveMetastoreURI;
  private String hiveServerURI;
  private String baseDir;
  private String hiveDb;
  private String hiveTable;
  private Map<String, String> partitions;

  public HiveSink() {
  }

  public HiveSink(String baseDir, String hiveDb, String hiveTable, Schema schema, Map<String, String> partitions) {
    super(schema);
    this.baseDir = baseDir;
    this.hiveDb = hiveDb;
    this.hiveTable = hiveTable;
    this.partitions = partitions;
  }

  @Override
  public Map<String, String> getConfiguration() {
    Map<String, String> config = Maps.newHashMap(super.getConfiguration());
    config.put(Constants.Batch.Sink.Hive.ARG_BASE_DIR, baseDir);
    config.put(Constants.Batch.Sink.Hive.ARG_HIVE_DB, hiveDb);
    config.put(Constants.Batch.Sink.Hive.ARG_HIVE_TABLE, hiveTable);
    config.put(Constants.Batch.Sink.Hive.ARG_PARTITIONS, GSON.toJson(partitions));
    return config;
  }

  @Override
  public void prepareJob(MapReduceContext context) throws IOException {
    super.prepareJob(context);

    // TODO: read from hive-site.xml
    hiveMetastoreURI = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_HIVE_METASTORE_URI);
    hiveServerURI = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_HIVE_SERVER_URI);

    baseDir = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_BASE_DIR);

    hiveDb = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_HIVE_DB);
    hiveTable = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_HIVE_TABLE);

    partitions = GSON.fromJson(Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_PARTITIONS),
                               new TypeToken<Map<String, String>>() {
                               }.getType());

    Job job = context.getHadoopJob();

    try {
      this.setOutput(job, hiveMetastoreURI, hiveServerURI, hiveDb, hiveTable, getSchema(), partitions);
    } catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  @Override
  protected void write(Mapper.Context context, Record record, Schema schema) throws IOException, InterruptedException {
    HCatSchema hCatSchema = HiveTableSchema.generateHCatSchema(schema);
    HCatRecord output = new DefaultHCatRecord(schema.getFields().size());

    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(hCatSchema);
    Preconditions.checkNotNull(output);

    for (Field field : schema.getFields()) {
      FieldType type = field.getType();
      String fieldName = field.getName();
      byte[] value = record.getValue(fieldName);

      String hiveFieldName = fieldName.toLowerCase();
      Preconditions.checkNotNull(hiveFieldName);
      Preconditions.checkNotNull(value);

      output.set(hiveFieldName, hCatSchema, type.fromBytes(value));
    }

    context.write(null, output);
  }


  @Override
  public void initialize(MapReduceContext context) throws IOException {
    super.initialize(context);

    baseDir = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_BASE_DIR);
    hiveDb = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_HIVE_DB);
    hiveTable = Programs.getRequiredArgOrProperty(context, Constants.Batch.Sink.Hive.ARG_HIVE_TABLE);
  }

  private void setOutput(Job job, String hiveMetastoreUri, String hiveServerUri, String dbName, String tableName,
                         Schema tableSchema, Map<String, String> partitions) throws Exception {

    // TODO: remove manually setting metastore uris
    job.getConfiguration().set(HiveConf.ConfVars.METASTOREURIS.varname, hiveMetastoreUri);
    createHiveTableIfNotExists(hiveServerUri, job.getConfiguration(),
                               baseDir, dbName, tableName, tableSchema, partitions);

    job.setOutputFormatClass(HCatOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(HCatRecord.class);

    // TODO: allow user specify format for default partition
    partitions = Maps.newHashMap(partitions);
    partitions.put("cty_timestamp", String.valueOf(System.currentTimeMillis()));
    OutputJobInfo outputJobInfo = OutputJobInfo.create(dbName, tableName, partitions);
    HCatOutputFormat.setOutput(job, outputJobInfo);

    HCatSchema s = HCatOutputFormat.getTableSchema(job.getConfiguration());
    HCatOutputFormat.setSchema(job, s);
  }

  private boolean createHiveTableIfNotExists(String hiveJdbcUrl, Configuration configuration,
                                             String baseDir, String dbName, String tableName,
                                             Schema schema, Map<String, String> partitions) throws Exception {

    Driver[] hiveDrivers = new Driver[] {
      new org.apache.hadoop.hive.jdbc.HiveDriver(),
      new org.apache.hive.jdbc.HiveDriver()
    };

    for (Driver driver : hiveDrivers) {
      Class.forName(driver.getClass().getName());
      DriverManager.registerDriver(driver);
    }

    Connection connection = DriverManager.getConnection(hiveJdbcUrl + "/" + dbName, "", "");

    String showTablesLikeQuery = String.format("show tables like '%s'", tableName);
    if (!connection.createStatement().executeQuery(showTablesLikeQuery).next()) {

      List<String> colStrings = new LinkedList<String>();

      List<String> partitionStrings = new LinkedList<String>();
      // adding default "by time" partition
      partitionStrings.add("cty_timestamp BIGINT");
      for (String partitionKey : partitions.keySet()) {
        // TODO: support other types of partition columns
        partitionStrings.add(partitionKey + " STRING");
      }

      for (Field field : schema.getFields()) {
        HiveFieldType hiveFieldType = HiveFieldType.fromFieldType(field.getType());
        colStrings.add(field.getName() + " " + hiveFieldType.getTypeString());
      }

      baseDir = baseDir.endsWith("/") ? baseDir : baseDir + "/";

      Path path = new Path(baseDir + dbName + "/" + tableName);
      FileSystem fs = path.getFileSystem(configuration);
      fs.mkdirs(path);

      String query =
        "create table " + tableName + " (" + Joiner.on(", ").join(colStrings) + ")"
          + (!partitionStrings.isEmpty() ? " partitioned by (" + Joiner.on(", ").join(partitionStrings) + ")" : "")
          + " row format delimited"
          + " fields terminated by '\t'"
          + " location '" + baseDir + dbName + "/" + tableName + "'";
      LOG.info("Executing query to create Hive table:\n{}", query);
      connection.createStatement().execute(query);
      return true;
    }

    return false;
  }
}
