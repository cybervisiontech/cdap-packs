package com.continuuity.att.dictionaryproc;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.dictionary.DictionaryDataSet;
import com.continuuity.lib.etl.schema.FieldType;
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
