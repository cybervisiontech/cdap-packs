package com.continuuity.lib.etl;

import com.continuuity.api.app.AbstractApplication;
import com.continuuity.api.dataset.Dataset;
import com.continuuity.api.dataset.DatasetProperties;
import com.google.common.collect.Sets;

import java.util.Set;

/**
 * Helps define {@link com.continuuity.api.app.Application} class that tracks all datasets added to it.
 */
public abstract class DatasetsTrackingApplication extends AbstractApplication {
  // we need to call useDataset() in MR & Flow - otherwise it will not work, hence this code
  // todo: check that we have to have it
  private final Set<String> datasets = Sets.newHashSet();

  @Override
  protected void createDataset(String datasetName, String typeName) {
    super.createDataset(datasetName, typeName);
    datasets.add(datasetName);
  }

  @Override
  protected void createDataset(String datasetName, String typeName, DatasetProperties properties) {
    super.createDataset(datasetName, typeName, properties);
    datasets.add(datasetName);
  }

  @Override
  protected void createDataset(String datasetName, Class<? extends Dataset> datasetClass, DatasetProperties props) {
    super.createDataset(datasetName, datasetClass, props);
    datasets.add(datasetName);
  }

  @Override
  protected void createDataset(String datasetName, Class<? extends Dataset> datasetClass) {
    super.createDataset(datasetName, datasetClass);
    datasets.add(datasetName);
  }

  protected Set<String> getDatasets() {
    return datasets;
  }
}
