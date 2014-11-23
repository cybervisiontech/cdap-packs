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

package co.cask.cdap.packs.etl.realtime;

import co.cask.cdap.packs.etl.TestConstants;
import co.cask.cdap.packs.etl.realtime.sink.DictionarySink;
import co.cask.cdap.packs.etl.realtime.source.SchemaSource;
import co.cask.cdap.packs.etl.transform.schema.DefaultSchemaMapping;

// Source, transformation and sink types configured by app code, other stuff configured by arguments
public class RealtimeETLConfigureByCode extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    configurer.setInputStream("userDetailsStream");
    configurer.setSource(new SchemaSource(TestConstants.getInSchema()));
    configurer.setTransformation(new DefaultSchemaMapping(TestConstants.getInSchema(),
                                                          TestConstants.getOutSchema(),
                                                          TestConstants.getMapping()));
    configurer.setSink(new DictionarySink("users", "user_id"));
  }
}
