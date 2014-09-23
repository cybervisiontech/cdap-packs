ETL-pack Library
================

ETL-pack library is designed to ease the development of common ETL solutions and provide powerful extension
capabilities. Developers can use it as is without any coding required to perform standard tasks like
incremental moving of data between Hive tables with applied transformation using input/ouput schema mapping and more.

At the same time developers can implement their own source and sink types as well as custom transformation logic. 
ETL-pack provides necessary abstractions and base building blocks for developer to focus on business logic and not 
infrastructure and operation boilerplate. Developed components are transferrable and re-usable in many different 
environment with CDAP.

Batch and Realtime ETL
----------------------

Currently ETL-pack supports two types of ETL pipelines: batch and real-time. Batch processing happens with help of 
MapReduce jobs. Real-time processing utilizes Tigon Flows container. There are number of configuration options 
for ETL pipeline as displayed below.

|(Batch)|

|(Realtime)|


The base class for batch ETL is BatchETL. Code below shows example application developed using BatchETL which 
pre-defined configuration:

.. code:: java

  public class StreamToHBaseETL extends BatchETL {
    @Override
    protected void configure(BatchETL.Configurer configurer) {
      configurer.addStream(new Stream("userDetails"));
      configurer.setSource(new StreamSource(getInSchema(), "userDetails"));
      configurer.setTransformation(
                 new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
      configurer.setSink(new HBaseSink("my.zk.quorum.host.net", "2181", "/hbase",
                                       "users-table", "colfam", "user_id"));
    }
 
    private Schema getInSchema() {
      return new Schema(ImmutableList.of(new Field("userId", FieldType.STRING),
                                         new Field("firstName", FieldType.STRING),
                                         new Field("lastName", FieldType.STRING)));
    }
  
    private Schema getOutSchema() {
      return new Schema(ImmutableList.of(new Field("user_id", FieldType.INT),
                                         new Field("name", FieldType.STRING)));
    }
  
    private ImmutableMap<String, String> getMapping() {
      return ImmutableMap.of(“user_id”, "userId",
                             "name", "first_name + ' ' + last_name");
    }
  }

Note that BatchETL application can be used as is, by providing all the configuration (e.g. the one hard-coded in 
code sample above) with runtime arguments in JSON format. The latter method is used by Subscription Management 
webapp to avoid rebuilding application jar for different subscription configurations.

The base class for real-time ETL is RealtimeETL. The following code shows example application developed using 
BatchETL which pre-defined configuration

.. code:: java

  public class StreamToKafkaETL extends RealtimeETL {
    @Override
    protected void configure(RealtimeETL.Configurer configurer) {
      configurer.addStream(new Stream("userDetails"));
      configurer.setSource(new SchemaSource(getInSchema(), "userDetails"));
      configurer.setTransformation(
        new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
      configurer.setSink(new KafkaSink("my.zk.quorum.host.net:2181", "my-kafka-topic"));
    }
  
    private Schema getInSchema() {...}
  
    private Schema getOutSchema() {...}
  
    private ImmutableMap<String, String> getMapping() {...}
  }
  
Note that similarly to BatchETL application the RealtimeETL app can be used as is, by providing the configuration 
with runtime argumets in JSON format.

Source
------

ETL-pack comes with number of sources available out of the box, like TableSource, StreamSource, MetadataSource which 
can be used in real-time and batch ETL. It also comes with higher-level abstractions and base classes to ease 
implementing custom source. The following code shows the interfaces to implement for real-time and batch cases.

.. code:: java

  public interface RealtimeSource extends ConfigurableProgram<FlowletContext> {
    Record read(StreamEvent streamEvent) throws Exception;
  }

.. code:: java

  public interface MapReduceSource<KEY_TYPE, VALUE_TYPE> extends ConfigurableProgram<MapReduceContext> {
    void prepareJob(MapReduceContext context);
    void onFinish(boolean succeeded, MapReduceContext context) throws Exception;
    Iterator<Record> read(KEY_TYPE key, VALUE_TYPE value);
  }

Example below shows implementation of standard MetadataSource to give an example of how easy it is 
to implement a custom source.

.. code:: java

  public class MetadataSource extends AbstractConfigurableProgram<FlowletContext> implements RealtimeSource {  
    @Override
    public Record read(StreamEvent streamEvent) throws Exception {
      Record.Builder builder = new Record.Builder();
      for (Map.Entry<String, String> header : streamEvent.getHeaders().entrySet()) {
        builder.add(header.getKey(), header.getValue().getBytes(Charsets.UTF_8));
      }
      return builder.build();
    }
  }

Transformation
--------------

ETL-pack comes with number of transformation options available out of the box, like IdentityTransformation, 
ScriptableSchemaMapping, etc. It also comes with higher-level abstractions and base classes to ease implementing 
custom source. Code below shows the interface to implement for transformation.

.. code:: java

  public interface Transformation extends ConfigurableProgram<RuntimeContext> {
    @Nullable
    Record transform(Record input) throws IOException, InterruptedException;
  }

Example below shows implementation of standard MetadataSource to give an example of how easy it is to 
implement a custom source.

.. code:: java

  public class FilterByFields extends AbstractConfigurableProgram<RuntimeContext> implements Transformation {
    public static final String ARG_INCLUDE_BY = "etl.transform.filterByFields.includeBy";
  
    private Map<String, String> includeBy;
  
    @Override
    public void initialize(RuntimeContext context) {
      String includeByArg = Programs.getRequiredArgOrProperty(context, ARG_INCLUDE_BY);
      this.includeBy = new Gson().fromJson(includeByArg, Map.class);
    }
  
    @Nullable
    @Override
    public Record transform(Record input) {
      for (Map.Entry<String, String> mustHave : includeBy.entrySet()) {
        if (!mustHave.getValue().equals(input.getValue(mustHave.getKey()))) {
          return null;
        }
      }
      return input;
    }
  }
  
Example above demonstrates integration of the ETL component with ETL program lifecycle. 
The FilterByFields uses required fields with values passed by user on ETL program start.

Sink
----

ETL-pack comes with number of sinks available out of the box, like HiveSink, KafkaSink, 
HBaseSink, DictionarySink which can be used in real-time and batch ETL. It also comes with 
higher-level abstractions and base classes to ease implementing custom sink. Code samples below 
show the interfaces to implement for real-time and batch cases.

.. code:: java

  public interface RealtimeSink extends ConfigurableProgram<FlowletContext> {
    void write(Record value) throws Exception;
  }
  
.. code:: java

  public interface MapReduceSink extends ConfigurableProgram<MapReduceContext> {
    void prepareJob(MapReduceContext context) throws IOException;
    void write(Mapper.Context context, Record value) throws IOException, InterruptedException;
  }
  
Similarly to Source and Transformation, Sink can be integrated CDAP acpplication components lifecycle to 
e.g. use run-time user arguments.

Unit-testing
------------

CDAP provides extensive support for create productive development environment, 
which includes unit-tests framework for testing both application components and application as a whole. 
Code below shows example of unit-test of the application that was introdiced above.

.. code:: java

  public class MyApplicationTest extends TestBase {
    private static HBaseTestBase testHBase;
  
    @BeforeClass
    public static void beforeClass() throws Exception {
      testHBase = new HBaseTestFactory().get();
      testHBase.startHBase();
    }
  
    @AfterClass
    public static void afterClass() throws Exception {
      testHBase.stopHBase();
    }
  
    @Test
    private void testETL() throws Exception {
      // deploy etl app
      ApplicationManager applicationManager = deployApplication(MyApplication.class);
      StreamWriter streamWriter = applicationManager.getStreamWriter("userDetails");
      streamWriter.send("1,Jack,Brown");
  
      // run etl job
      Map<String, String> args = ImmutableMap.of(HBaseSink.ARG_ZK,
                                                 testHBase.getZkConnectionString());
      MapReduceManager mr = applicationManager.startMapReduce("BatchETLMapReduce", args);
      mr.waitForFinish(2, TimeUnit.MINUTES);
  
      // verify results
      HTable hTable = testHBase.getHTable("users-table");
      Result result = hTable.get(new Get(Bytes.toBytes(1)));
      Assert.assertFalse(result.isEmpty());
      Assert.assertEquals("Jack Brown",
                          result.getValue(Bytes.toBytes("colfam"), Bytes.toBytes("name")));
    }
  }

In this example unit-test uses HBaseTestBase utility provided by unit-testing framework to test output
into external HBase table using HBaseSink. When only internal Reactor components (like DataSets) are 
used by the application, unit-tests are simplified even further, as shown in code below.

.. code:: java

  public class MyApplicationTest extends ReactorTestBase {
    @Test
    private void testETL() throws Exception {
      // deploy etl app
      ApplicationManager applicationManager = deployApplication(MyApplication.class);
      StreamWriter streamWriter = applicationManager.getStreamWriter("userDetails");
      streamWriter.send("1,Jack,Brown");
  
      // run etl job
      MapReduceManager mr = applicationManager.startMapReduce("BatchETLMapReduce");
      mr.waitForFinish(2, TimeUnit.MINUTES);
  
      // verify results
      DictionaryDataSet dictionary = appMngr.getDataSet(Constants.DICTIONARY_DATASET).get();
      Assert.assertEquals("Jack Brown",
                          Bytes.toString(dictionary.get("users", Bytes.toBytes(1), "name")));
    }
  }

In this example we test same application but with sink changed to DictionarySink 
which can be used for lookup during data transformation. Note that unit-test framework provides 
in-memory runtime for datasets for fast execution.

License
=======

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

.. |(Batch)| image:: docs/img/batch.png

.. |(Realtime)| image:: docs/img/realtime.png
