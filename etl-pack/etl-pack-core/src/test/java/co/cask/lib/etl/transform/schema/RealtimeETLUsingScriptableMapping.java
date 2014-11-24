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

package co.cask.lib.etl.transform.schema;

import co.cask.lib.etl.realtime.RealtimeETL;
import co.cask.lib.etl.realtime.sink.DictionarySink;
import co.cask.lib.etl.realtime.source.SchemaSource;

import static co.cask.lib.etl.transform.schema.SchemaTestConstants.getInSchema;
import static co.cask.lib.etl.transform.schema.SchemaTestConstants.getMapping;
import static co.cask.lib.etl.transform.schema.SchemaTestConstants.getOutSchema;

public class RealtimeETLUsingScriptableMapping extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    configurer.setInputStream("usersStream");
    configurer.setSource(new SchemaSource(getInSchema()));
    configurer.setTransformation(new ScriptableSchemaMapping(getInSchema(), getOutSchema(), getMapping()));
    configurer.setSink(new DictionarySink("myDict", "user_id"));
  }
}
