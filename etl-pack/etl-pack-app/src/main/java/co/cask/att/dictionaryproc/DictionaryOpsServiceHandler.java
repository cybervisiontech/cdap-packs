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

package co.cask.att.dictionaryproc;

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import co.cask.lib.etl.Constants;
import co.cask.lib.etl.dictionary.DictionaryDataSet;
import co.cask.lib.etl.schema.FieldType;
import com.google.gson.JsonObject;
import org.mortbay.jetty.MimeTypes;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * Dictionary ops service handler
 */
public class DictionaryOpsServiceHandler extends AbstractHttpServiceHandler {

  @UseDataSet(Constants.DICTIONARY_DATASET)
  private DictionaryDataSet dictionary;

  /**
   * Get a value from dictionary by specific key. Request should be formatted by pattern:
   * get/{dictionary name}/{dictionary key}/{dictionary field}/{dictionary type}
   *
   * @param request
   * @param responder
   * @param dictName Name of dictionary where value must be searched
   * @param dictKey Key for search
   * @param dictField Field of dictionary
   * @param type Type of key. Supported values: STRING, INT, DOUBLE, FLOAT, LONG.
   *             See {@link FieldType} for list of supported types.
   * @throws IOException
   */
  @Path("get/{dictName}/{dictKey}/{dictField}/{keyType}")
  @GET
  public void get(HttpServiceRequest request, HttpServiceResponder responder, @PathParam("dictName") String dictName,
                  @PathParam("dictKey") String dictKey, @PathParam("dictField") String dictField,
                  @PathParam("keyType") String type) throws IOException {
    FieldType keyType = FieldType.valueOf(type.toUpperCase());

    byte[] val = dictionary.get(dictName, keyType.toBytes(dictKey), dictField);
    if (val == null || val.length == 0) {
      responder.sendJson(new JsonObject());
    } else {
      responder.send(HttpURLConnection.HTTP_OK, ByteBuffer.wrap(val), MimeTypes.TEXT_PLAIN, null);
    }
  }
}
