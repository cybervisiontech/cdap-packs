package com.continuuity.packs.etl;

import com.continuuity.api.data.stream.Stream;

/**
 *
 */
public class TestDataDictionary extends SimpleDataDictionary {
  @Override
  public String getDataDictionaryName() {
    return "foo";
  }

  @Override
  public Stream getStream() {
    return new Stream("test");
  }

  @Override
  public String getDictionaryKey(String event) {
    return event;
  }

  @Override
  public String getDictionaryValue(String event) {
    return event;
  }
}
