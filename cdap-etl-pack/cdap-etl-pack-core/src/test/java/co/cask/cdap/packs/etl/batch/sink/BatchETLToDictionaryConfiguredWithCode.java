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

package co.cask.cdap.packs.etl.batch.sink;

import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.packs.etl.batch.BatchETL;
import co.cask.cdap.packs.etl.batch.source.TableSource;
import co.cask.cdap.packs.etl.transform.schema.DefaultSchemaMapping;

import static co.cask.cdap.packs.etl.TestConstants.getInSchema;
import static co.cask.cdap.packs.etl.TestConstants.getMapping;
import static co.cask.cdap.packs.etl.TestConstants.getOutSchema;

public final class BatchETLToDictionaryConfiguredWithCode extends BatchETL {
  @Override
  protected void configure(Configurer configurer) {
    createDataset("userDetails2", Table.class);
    configurer.setSource(new TableSource(getInSchema(), "userDetails2"));
    configurer.setTransformation(new DefaultSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    configurer.setSink(new DictionarySink("users2", "user_id"));
  }
}
