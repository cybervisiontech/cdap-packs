package com.continuuity.lib.etl.transform.script;

import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.schema.FieldType;
import junit.framework.Assert;
import org.junit.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class LookupFunctionTest {

  @Test
  public void shouldPass() {
    int wifiId = 123;
    byte[] wifiIdBytes = FieldType.INT.toBytes(wifiId);

    String someWifiName = "some_wifi_name";
    byte[] someWifiNameBytes = FieldType.STRING.toBytes(someWifiName);

    String dictionaryName = "wifi_venues";
    String fieldName = "wifi_name";

    DictionaryDataSet table = mock(DictionaryDataSet.class);
    when(table.get(eq(dictionaryName), eq(wifiIdBytes), eq(fieldName)))
      .thenReturn(someWifiNameBytes);

    Record record = new Record.Builder()
      .add("wifi_id", wifiIdBytes)
      .add("dummy", FieldType.DOUBLE.toBytes(123.123))
      .build();

    LookupFunction lookupFunction = new LookupFunction(table, record);

    Assert.assertEquals(someWifiName, lookupFunction.lookup(dictionaryName, "wifi_id", fieldName, "undefined"));
  }

}
