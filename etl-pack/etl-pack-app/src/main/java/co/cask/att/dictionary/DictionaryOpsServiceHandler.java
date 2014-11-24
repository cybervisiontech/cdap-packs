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

package co.cask.att.dictionary;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.dictionary.DictionaryDataSet;
import co.cask.lib.etl.schema.FieldType;
import com.google.common.base.Charsets;
import org.mortbay.jetty.MimeTypes;

import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Dictionary ops service handler.
 */
public class DictionaryOpsServiceHandler extends AbstractHttpServiceHandler {

  @UseDataSet(Constants.DICTIONARY_DATASET)
  private DictionaryDataSet dictionary;

  /**
   * Retrieves a value from a dictionary for a specific key. Request should be formatted using this pattern:<br>
   * <code>get/{dictionary-name}/{dictionary-key}/{dictionary-field}/{dictionary-key-type}</code>
   *
   * @param request   HTTP service request
   * @param responder HTTP service responder
   * @param dictName  Name of dictionary where value must be searched
   * @param dictKey   Key for search
   * @param dictField Field of dictionary
   * @param keyType   Type of key. Supported types: STRING, INT, DOUBLE, FLOAT, LONG.
   *                  See {@link FieldType} for list of supported types.
   */
  @Path("get/{dictName}/{dictKey}/{dictField}/{keyType}")
  @GET
  public void get(HttpServiceRequest request, HttpServiceResponder responder,
                  @PathParam("dictName") String dictName, @PathParam("dictKey") String dictKey,
                  @PathParam("dictField") String dictField, @PathParam("keyType") String keyType) {
    FieldType type = FieldType.valueOf(keyType.toUpperCase());

    byte[] dict = dictionary.get(dictName, type.toBytes(dictKey), dictField);
    if (dict == null || dict.length == 0) {
      responder.sendString(HttpURLConnection.HTTP_NO_CONTENT, "No dictionary found", Charsets.UTF_8);
    } else {
      responder.send(HttpURLConnection.HTTP_OK, ByteBuffer.wrap(dict), MimeTypes.TEXT_PLAIN, null);
    }
  }
}
