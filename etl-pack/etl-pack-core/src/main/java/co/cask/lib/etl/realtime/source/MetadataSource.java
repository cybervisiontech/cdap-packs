/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.lib.etl.realtime.source;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.flow.flowlet.FlowletContext;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.lib.etl.AbstractConfigurableProgram;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.Record;
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
