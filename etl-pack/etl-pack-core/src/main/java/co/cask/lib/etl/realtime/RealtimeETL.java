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

package co.cask.lib.etl.realtime;

import co.cask.cdap.api.data.stream.Stream;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.DatasetsTrackingApplication;
import co.cask.lib.etl.batch.ETLMetaDataSet;
import co.cask.lib.etl.dictionary.DictionaryDataSet;
import co.cask.lib.etl.realtime.sink.RealtimeSink;
import co.cask.lib.etl.realtime.source.RealtimeSource;
import co.cask.lib.etl.transform.Transformation;

/**
 * Simple application for real-time ETL pipeline.
 */
public class RealtimeETL extends DatasetsTrackingApplication {

  public void configure() {
    // defaults, can be overridden in configure()
    setName(this.getClass().getSimpleName());
    setDescription("Real-time ETL Application");
    createDataset(Constants.DICTIONARY_DATASET, DictionaryDataSet.class);
    createDataset(Constants.ETL_META_DATASET, ETLMetaDataSet.class);

    final Configurer configurer = new Configurer();
    configure(configurer);

    String inStreamName = configurer.stream == null ? Constants.DEFAULT_INPUT_STREAM : configurer.stream;
    addStream(new Stream(inStreamName));


    ETLFlow flow = new ETLFlow(inStreamName, configurer.source,
                               configurer.transformation, configurer.sink, getDatasets());

    addFlow(flow);
  }

  protected void configure(Configurer configurer) {
    // do nothing by default
  }

  public static final class Configurer {
    private RealtimeSource source = null;
    private Transformation transformation = null;
    private RealtimeSink sink = null;

    private String stream = null;

    public void setInputStream(String stream) {
      this.stream = stream;
    }

    public void setSource(RealtimeSource source) {
      this.source = source;
    }

    public void setTransformation(Transformation transformation) {
      this.transformation = transformation;
    }

    public void setSink(RealtimeSink sink) {
      this.sink = sink;
    }
  }
}
