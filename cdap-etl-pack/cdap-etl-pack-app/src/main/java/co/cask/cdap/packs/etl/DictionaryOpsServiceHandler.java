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

package co.cask.cdap.packs.etl;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.packs.etl.schema.FieldType;
import com.google.common.base.Charsets;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Dictionary operations service handler.
 */
public class DictionaryOpsServiceHandler extends AbstractHttpServiceHandler {

  @UseDataSet(Constants.DICTIONARY_DATASET)
  private DictionaryDataSet dictionary;

  /**
   * Retrieves a value from a dictionary for a specific key. Request should be formatted using this pattern:<br>
   * <code>get/{dictionary-name}/{dictionary-key}/{dictionary-field}/{dictionary-field-type}</code>
   *
   * @param request HTTP service request
   * @param responder HTTP service responder
   * @param dictName Name of dictionary where value must be searched
   * @param dictKey Key for search
   * @param dictField Field of dictionary
   * @param fieldType Type of field. Supported types: STRING, INT, LONG, FLOAT, DOUBLE.
   *                  See {@link FieldType} for list of supported types.
   */
  @Path("get/{dictName}/{dictKey}/{dictField}/{fieldType}")
  @GET
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("dictName") String dictName, @PathParam("dictKey") String dictKey,
                  @PathParam("dictField") String dictField, @PathParam("fieldType") String fieldType) {
    FieldType type;
    try {
      type = FieldType.valueOf(fieldType.toUpperCase());
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpURLConnection.HTTP_BAD_REQUEST);
      return;
    }

    byte[] value = dictionary.get(dictName, Bytes.toBytes(dictKey), dictField);
    if (value == null || value.length == 0) {
      responder.sendStatus(HttpURLConnection.HTTP_NO_CONTENT);
    } else {
      responder.sendString(HttpURLConnection.HTTP_OK, String.valueOf(type.fromBytes(value)), Charsets.UTF_8);
    }
  }
}
