package com.continuuity.lib.etl.transform.schema;

import com.continuuity.api.common.Bytes;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.schema.Field;
import com.continuuity.lib.etl.schema.FieldType;
import com.continuuity.lib.etl.schema.Schema;
import com.continuuity.lib.etl.transform.script.LookupFunction;
import com.continuuity.lib.etl.transform.script.ScriptBasedTransformer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.script.ScriptException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class ScriptableSchemaMappingTest {

  private static final Logger LOG = LoggerFactory.getLogger(ScriptableSchemaMappingTest.class);
  private static final Gson GSON = new Gson();

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void shouldPassWithFieldTypeStringInLookup() throws ScriptException {
    String expectedUser = "testvalue1";
    byte[] expectedUserBytes = FieldType.STRING.toBytes(expectedUser);

    int wifiId = 321;
    byte[] wifiIdBytes = FieldType.INT.toBytes(wifiId);

    double expectedWifiLat = 123.321;

    DictionaryDataSet dictionaryDataSet = mock(DictionaryDataSet.class);
    when(dictionaryDataSet.get(eq("testdict"), any(byte[].class), eq("wifi_lat")))
      .thenReturn(FieldType.DOUBLE.toBytes(expectedWifiLat));

    Record input = new Record.Builder()
      .add("user", expectedUserBytes)
      .add("wifi_id", wifiIdBytes)
      .build();

    Schema inputSchema = new Schema(
      new Field("user", FieldType.STRING),
      new Field("wifi_id", FieldType.INT)
    );

    Schema outputSchema = new Schema(
      new Field("user", FieldType.STRING),
      new Field("wifi_lat", FieldType.DOUBLE)
    );

    Map<String, String> mapping = ImmutableMap.<String, String>builder()
      .put("wifi_lat", "lookup('testdict', 'wifi_id', 'wifi_lat', 'DOUBLE')")
      .put("user", "user")
      .build();

    ScriptBasedTransformer transformer = new ScriptBasedTransformer();
    LookupFunction lookupFunction = new LookupFunction(dictionaryDataSet, input);
    Record transform = transformer.transform(input, inputSchema, outputSchema,
                                             mapping, lookupFunction.getContextVariables());

    Assert.assertEquals(2, transform.getFields().size());
    Assert.assertEquals(expectedWifiLat, FieldType.DOUBLE.fromBytes(transform.getValue("wifi_lat")));
    Assert.assertEquals(expectedUser, FieldType.STRING.fromBytes(transform.getValue("user")));
  }

  @Test
  public void shouldPassWithoutFieldTypeStringInLookup() throws ScriptException {
    int wifiId = 321;
    byte[] wifiIdBytes = FieldType.INT.toBytes(wifiId);

    double expectedWifiLat = 123.321;
    String expectedWifiName = "some_wifi_name";
    String expectedUser = "testvalue1";
    byte[] expectedUserBytes = FieldType.STRING.toBytes(expectedUser);

    DictionaryDataSet dictionaryDataSet = mock(DictionaryDataSet.class);
    when(dictionaryDataSet.get(eq("testdict"), any(byte[].class), eq("wifi_name")))
      .thenReturn(FieldType.STRING.toBytes(expectedWifiName));

    Record input = new Record.Builder()
      .add("user", expectedUserBytes)
      .add("wifi_id", wifiIdBytes)
      .build();

    Schema inputSchema = new Schema(
      new Field("user", FieldType.STRING),
      new Field("wifi_id", FieldType.INT)
    );

    Schema outputSchema = new Schema(
      new Field("user", FieldType.STRING),
      new Field("wifi_name", FieldType.STRING)
    );

    Map<String, String> mapping = ImmutableMap.<String, String>builder()
      .put("wifi_name", "lookup('testdict', 'wifi_id', 'wifi_name')")
      .put("user", "user")
      .build();

    ScriptBasedTransformer transformer = new ScriptBasedTransformer();
    LookupFunction lookupFunction = new LookupFunction(dictionaryDataSet, input);
    Record transform = transformer.transform(input, inputSchema, outputSchema,
                                             mapping, lookupFunction.getContextVariables());

    Assert.assertEquals(2, transform.getFields().size());
    Assert.assertEquals(expectedWifiName, FieldType.STRING.fromBytes(transform.getValue("wifi_name")));
    Assert.assertEquals(expectedUser, FieldType.STRING.fromBytes(transform.getValue("user")));
  }

  @Test
  public void shouldSucceedWithSimpleTransformation() throws ScriptException {
    doTestTransform(getDefaultTransformTestCase());
  }

  @Test
  public void shouldThrowScriptExceptionWhenMissingInputField() throws ScriptException {
    TransformTestCase testCase = getDefaultTransformTestCase();
    int prevSize = testCase.getInputSchemaFields().size();
    Field removedField = testCase.getInputSchemaFields().remove(0);
    Assert.assertEquals(prevSize - 1, testCase.getInputSchemaFields().size());

    expectedEx.expect(ScriptException.class);
    expectedEx.expectMessage("\"" + removedField.getName() + "\" is not defined.");
    doTestTransform(testCase);
  }

  @Test
  public void shouldSucceedWithMissingOutputField() throws ScriptException {
    TransformTestCase testCase = getDefaultTransformTestCase();
    int prevSize = testCase.getOutputSchemaFields().size();
    testCase.getOutputSchemaFields().remove(0);
    Assert.assertEquals(prevSize - 1, testCase.getOutputSchemaFields().size());

    doTestTransform(testCase);
  }

  @Test
  public void shouldSucceedWithMissingOutputMapping() throws ScriptException {
    TransformTestCase testCase = getDefaultTransformTestCase();
    int prevSize = testCase.getMappingBuilder().size();

    String removedOutputFieldName = testCase.getMappingBuilder().keySet().iterator().next();
    testCase.getMappingBuilder().remove(removedOutputFieldName);
    Assert.assertEquals(prevSize - 1, testCase.getMappingBuilder().size());

    testCase.getExpectedOutputBuilder().remove(removedOutputFieldName);
    Field removedOutputField = testCase.getOutputSchema().getField(removedOutputFieldName);
    FieldType type = removedOutputField.getType();
    testCase.getExpectedOutputBuilder().add(removedOutputFieldName, type.toBytes(type.getDefaultValue()));

    doTestTransform(testCase);
  }

  private void doTestTransform(TransformTestCase testCase) throws ScriptException {
    Schema inputSchema = testCase.getInputSchema();
    Schema outputSchema = testCase.getOutputSchema();
    Map<String, String> mapping = testCase.getMapping();
    Record input = testCase.getInput();
    Record expectedOutput = testCase.getExpectedOutput();

    LOG.info("Testing with\ninputSchema={}\noutputSchema={}\nmapping={}\ninput={}\nexpectedOutput={}",
             GSON.toJson(inputSchema), GSON.toJson(outputSchema), GSON.toJson(mapping),
             GSON.toJson(recordToMap(inputSchema, input)), GSON.toJson(recordToMap(outputSchema, expectedOutput)));

    ScriptBasedTransformer processor = new ScriptBasedTransformer();
    Record output = processor.transform(input, inputSchema, outputSchema, mapping);
    assertRecordEquals(outputSchema, expectedOutput, output);
  }

  private void assertRecordEquals(Schema schema, Record expected, Record actual) {
    Assert.assertEquals(recordToMap(schema, expected), recordToMap(schema, actual));
  }

  private Map<String, Object> recordToMap(Schema schema, Record record) {
    Map<String, Object> map = Maps.newHashMap();
    for (Field field : schema.getFields()) {
      FieldType type = field.getType();
      byte[] valueBytes = record.getValue(field.getName());
      if (valueBytes == null) {
        map.put(field.getName(), null);
      } else {
        map.put(field.getName(), type.fromBytes(valueBytes));
      }
    }
    return map;
  }

  private static final TransformTestCase getDefaultTransformTestCase() {
    List<Field> inputSchema = Lists.newArrayList(ImmutableList.<Field>builder().add(new Field("someInt", FieldType.INT)).add(new Field("someString", FieldType.STRING)).add(new Field("someLong", FieldType.LONG)).build());

    List<Field> outputSchema = Lists.newArrayList(
      ImmutableList.<Field>builder()
        .add(new Field("someIntMod10", FieldType.INT))
        .add(new Field("someStringLen", FieldType.INT))
        .add(new Field("someStringPlusSomeLong", FieldType.STRING))
        .build());

    Map<String, String> mapping = Maps.newHashMap(
      ImmutableMap.<String, String>builder()
        .put("someIntMod10", "someInt % 10")
        .put("someStringLen", "someString.length")
        .put("someStringPlusSomeLong", "someString + someLong")
        .build());

    Record.Builder input = new Record.Builder()
      .add("someInt", Bytes.toBytes(123))
      .add("someString", Bytes.toBytes("HELLO WORLD~!"))
      .add("someLong", Bytes.toBytes(321L));

    Record.Builder expectedOutput = new Record.Builder()
      .add("someIntMod10", Bytes.toBytes(3))
      .add("someStringLen", Bytes.toBytes("HELLO WORLD~!".length()))
      .add("someStringPlusSomeLong", Bytes.toBytes("HELLO WORLD~!321"));

    return new TransformTestCase(inputSchema, outputSchema, mapping, input, expectedOutput);
  }

  private static class TransformTestCase {
    private final List<Field> inputSchemaFields;
    private final List<Field> outputSchemaFields;
    private final Map<String, String> mappingBuilder;
    private final Record.Builder inputBuilder;
    private final Record.Builder expectedOutputBuilder;

    private TransformTestCase(List<Field> inputSchemaFields,
                              List<Field> outputSchemaFields,
                              Map<String, String> mappingBuilder,
                              Record.Builder inputBuilder, Record.Builder expectedOutputBuilder) {
      this.inputSchemaFields = inputSchemaFields;
      this.outputSchemaFields = outputSchemaFields;
      this.mappingBuilder = mappingBuilder;
      this.inputBuilder = inputBuilder;
      this.expectedOutputBuilder = expectedOutputBuilder;
    }

    public Schema getInputSchema() {
      return new Schema(ImmutableList.copyOf(inputSchemaFields));
    }

    public Schema getOutputSchema() {
      return new Schema(ImmutableList.copyOf(outputSchemaFields));
    }

    public Map<String, String> getMapping() {
      return ImmutableMap.copyOf(mappingBuilder);
    }

    public Record getInput() {
      return inputBuilder.build();
    }

    public Record getExpectedOutput() {
      return expectedOutputBuilder.build();
    }

    public List<Field> getInputSchemaFields() {
      return inputSchemaFields;
    }

    public List<Field> getOutputSchemaFields() {
      return outputSchemaFields;
    }

    public Map<String, String> getMappingBuilder() {
      return mappingBuilder;
    }

    public Record.Builder getInputBuilder() {
      return inputBuilder;
    }

    public Record.Builder getExpectedOutputBuilder() {
      return expectedOutputBuilder;
    }
  }
}
