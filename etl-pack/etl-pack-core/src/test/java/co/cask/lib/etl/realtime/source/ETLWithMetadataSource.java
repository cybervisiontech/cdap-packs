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

import co.cask.lib.etl.realtime.RealtimeETL;
import co.cask.lib.etl.realtime.sink.DictionarySink;
import co.cask.lib.etl.transform.IdentityTransformation;

public class ETLWithMetadataSource extends RealtimeETL {
  @Override
  protected void configure(Configurer configurer) {
    configurer.setInputStream("filesStream");
    configurer.setSource(new MetadataSource());
    configurer.setTransformation(new IdentityTransformation());
    configurer.setSink(new DictionarySink("myDict", "filename"));
  }
}
