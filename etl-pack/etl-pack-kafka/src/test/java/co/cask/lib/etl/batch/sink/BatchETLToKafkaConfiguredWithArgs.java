/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.lib.etl.batch.sink;

import co.cask.cdap.api.dataset.table.Table;
import co.cask.lib.etl.batch.BatchETL;
import com.google.common.base.Preconditions;

// Everything configured by args
public final class BatchETLToKafkaConfiguredWithArgs extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    // hack to make reactor test framework package KafkaSink into the unit test app jar
    Preconditions.checkNotNull(KafkaSink.class);
    createDataset("userDetails1", Table.class);
  }
}
