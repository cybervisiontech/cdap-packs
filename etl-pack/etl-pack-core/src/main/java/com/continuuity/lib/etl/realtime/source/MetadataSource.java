package com.continuuity.lib.etl.realtime.source;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.lib.etl.AbstractConfigurableProgram;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Record;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Map;

/**
 * Treats {@link StreamEvent} headers as records fields.
 */
public class MetadataSource extends AbstractConfigurableProgram<FlowletContext> implements RealtimeSource {

  @Override
  public Iterator<Record> read(StreamEvent streamEvent) throws Exception {
    Record.Builder builder = new Record.Builder();
    for (Map.Entry<String, String> header : streamEvent.getHeaders().entrySet()) {
      builder.add(header.getKey(), header.getValue().getBytes(Charsets.UTF_8));
    }
    builder.add(Constants.Realtime.Source.Metadata.SIZE_FIELD,
                Bytes.toBytes(String.valueOf(streamEvent.getBody().remaining())));

    return Iterators.singletonIterator(builder.build());
  }
}
