package com.continuuity.lib.etl.batch;

import com.continuuity.api.data.stream.Stream;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.DatasetsTrackingApplication;
import com.continuuity.lib.etl.batch.sink.MapReduceSink;
import com.continuuity.lib.etl.batch.source.MapReduceSource;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.transform.Transformation;

/**
 * Simple application for Batch processing ETL pipeline.
 */
public class BatchETL extends DatasetsTrackingApplication {

  public void configure() {
    //setting defaults - can be overriden in configure()
    setName(this.getClass().getSimpleName());
    setDescription("Batch ETL Application");

    addStream(new Stream(Constants.DEFAULT_INPUT_STREAM));
    createDataset(Constants.DICTIONARY_DATASET, DictionaryDataSet.class);
    createDataset(Constants.ETL_META_DATASET, ETLMetaDataSet.class);

    Configurer configurer = new Configurer();
    configure(configurer);

    ETLMapReduce mapReduce = new ETLMapReduce(configurer.source,
                                              configurer.transformation,
                                              configurer.sink,
                                              getDatasets());

    // todo: this seems redundant: we add it as part of workflow
    addMapReduce(mapReduce);
    addWorkflow(new ETLMapReduceWorkflow(mapReduce));
  }

  protected void configure(Configurer configurer) {
    // do nothing by default
  }

  public static final class Configurer {
    private MapReduceSource source = null;
    private Transformation transformation = null;
    private MapReduceSink sink = null;

    public void setSource(MapReduceSource source) {
      this.source = source;
    }

    public void setTransformation(Transformation transformation) {
      this.transformation = transformation;
    }

    public void setSink(MapReduceSink sink) {
      this.sink = sink;
    }
  }
}
