package com.continuuity.packs.etl;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.google.common.base.Charsets;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

/**
 *
 */
public abstract class SimpleDataDictionary implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("DataDictionary")
      .setDescription("Simple Data Dictionary Implementation ETL")
      .withStreams()
        .add(getStream())
      .withDataSets()
        .add(new KeyValueTable(getDataDictionaryName()))
      .withFlows()
        .add(new DataDictionaryProcessor())
      .withProcedures()
        .add(new DataDictionaryProcedure())
      .noMapReduce()
      .noWorkflow()
      .build();
  }


  private final class DataDictionaryProcessor implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("DataDictionaryProcessor_" + getDataDictionaryName())
        .setDescription("Creating data dictionary " + getDataDictionaryName())
        .withFlowlets()
          .add(new DataDictionaryCreator())
        .connect()
          .from(getStream()).to(new DataDictionaryCreator())
        .build();
    }

    private final class DataDictionaryCreator extends AbstractFlowlet {
      @UseDataSet("foo")
      private KeyValueTable kvTable;

      @ProcessInput
      public void process(StreamEvent event) throws CharacterCodingException {
        ByteBuffer payload = event.getBody();
        String e = Charsets.UTF_8.newDecoder().decode(payload).toString();
        String k = getDictionaryKey(e);
        String v = getDictionaryValue(e);
        if(k != null && v != null) {
          kvTable.write(k, v);
        }
      }
    }
  }

  private final class DataDictionaryProcedure extends AbstractProcedure {
    @UseDataSet("foo")
    private KeyValueTable kvTable;

    @Override
    public String getName() {
      return "DataDictionaryProcedure_" + getDataDictionaryName();
    }

    @Handle("get")
    public void get(ProcedureRequest request, ProcedureResponder responder) throws IOException {
      String key = request.getArgument("key");
      if(key == null || key.isEmpty()) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "Request id not passed");
        return;
      }
      byte[] s = kvTable.read(key.getBytes(Charsets.UTF_8));
      if (s.length < 1) {
        responder.error(ProcedureResponse.Code.NOT_FOUND, "Request id not found");
        return;
      }
      responder.sendJson(Bytes.toString(s));
    }
  }


  public abstract String getDataDictionaryName();
  public abstract Stream getStream();
  public abstract String getDictionaryKey(String event);
  public abstract String getDictionaryValue(String event);
}
