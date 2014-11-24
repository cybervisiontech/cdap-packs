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

package co.cask.cdap.packs.etl;

import co.cask.cdap.api.annotation.Handle;
import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.procedure.AbstractProcedure;
import co.cask.cdap.api.procedure.ProcedureRequest;
import co.cask.cdap.api.procedure.ProcedureResponder;
import co.cask.cdap.api.procedure.ProcedureResponse;
import co.cask.cdap.packs.etl.dictionary.DictionaryDataSet;
import co.cask.cdap.packs.etl.schema.FieldType;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 *
 */
public class DictionaryOps extends AbstractProcedure {

  private static final String DICTIONARY_NAME = "name";
  private static final String DICTIONARY_KEY = "key";
  private static final String DICTIONARY_KEY_TYPE = "keyType";
  private static final String DICTIONARY_FIELD = "field";

  private static final Gson GSON = new Gson();

  @UseDataSet(Constants.DICTIONARY_DATASET)
  private DictionaryDataSet dictionary;

  @Handle("get")
  public void get(ProcedureRequest request, ProcedureResponder responder) throws IOException {
    String dictName = request.getArgument(DICTIONARY_NAME);
    String dictKey = request.getArgument(DICTIONARY_KEY);
    String dictField = request.getArgument(DICTIONARY_FIELD);
    FieldType keyType = GSON.fromJson(request.getArgument(DICTIONARY_KEY_TYPE), FieldType.class);

    byte[] val = dictionary.get(dictName, keyType.toBytes(dictKey), dictField);
    if (val.length == 0) {
      responder.sendJson(ProcedureResponse.Code.SUCCESS, new JsonObject());
    } else {
      ProcedureResponse.Writer writer =
          responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
      writer.write(ByteBuffer.wrap(val)).close();
    }
  }
}
