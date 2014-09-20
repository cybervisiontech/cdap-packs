package com.continuuity.lib.etl.realtime;

import com.continuuity.api.data.stream.Stream;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.DatasetsTrackingApplication;
import com.continuuity.lib.etl.batch.ETLMetaDataSet;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.realtime.sink.RealtimeSink;
import com.continuuity.lib.etl.realtime.source.RealtimeSource;
import com.continuuity.lib.etl.transform.Transformation;

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
