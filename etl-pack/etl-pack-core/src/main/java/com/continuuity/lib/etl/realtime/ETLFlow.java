package com.continuuity.lib.etl.realtime;

import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletContext;
import com.continuuity.api.flow.flowlet.FlowletSpecification;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.lib.etl.Constants;
import com.continuuity.lib.etl.Programs;
import com.continuuity.lib.etl.Record;
import com.continuuity.lib.etl.realtime.sink.RealtimeSink;
import com.continuuity.lib.etl.realtime.source.RealtimeSource;
import com.continuuity.lib.etl.transform.Transformation;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * {@link Flow} that performs ETL work
 */
public class ETLFlow implements Flow {
  // Optional: will be resolved via args
  private String inputStream = null;
  private RealtimeSource source = null;
  private Transformation transformation = null;
  private RealtimeSink sink = null;
  private Set<String> datasets = Collections.emptySet();

  public ETLFlow() {
  }

  public ETLFlow(String inputStream, RealtimeSource source, Transformation transformation,
                 RealtimeSink sink, Set<String> datasets) {
    this.inputStream = inputStream;
    this.source = source;
    this.transformation = transformation;
    this.sink = sink;
    this.datasets = datasets == null ? Collections.<String>emptySet() : datasets;
  }

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("ETLFlow").setDescription("")
      .withFlowlets().add("ETLFlowlet", new ETLFlowlet(source, transformation, sink, datasets))
      .connect()
      .from(new Stream(inputStream == null ? Constants.DEFAULT_INPUT_STREAM : inputStream)).to("ETLFlowlet")
      .build();
  }

  public static final class ETLFlowlet extends AbstractFlowlet {
    // Optional: will be resolved via args
    private RealtimeSource source = null;
    private Transformation transformation = null;
    private RealtimeSink sink = null;
    private Set<String> datasets = Collections.emptySet();

    public ETLFlowlet(RealtimeSource source, Transformation transformation, RealtimeSink sink, Set<String> datasets) {
      this.source = source;
      this.transformation = transformation;
      this.sink = sink;
      this.datasets = datasets;
    }

    @Override
    public FlowletSpecification configure() {
      Map<String, String> args = Maps.newHashMap();
      if (source != null) {
        args.put(Constants.Realtime.Source.ARG_SOURCE_TYPE, source.getClass().getName());
        args.putAll(source.getConfiguration());
      }
      if (transformation != null) {
        args.put(Constants.Realtime.Transformation.ARG_TRANSFORMATION_TYPE, transformation.getClass().getName());
        args.putAll(transformation.getConfiguration());
      }
      if (sink != null) {
        args.put(Constants.Realtime.Sink.ARG_SINK_TYPE, sink.getClass().getName());
        args.putAll(sink.getConfiguration());
      }

      FlowletSpecification.Builder.AfterDescription afterDescription = FlowletSpecification.Builder.with()
        .setName("ETLFlowlet").setDescription("")
        .withArguments(args)
        .useDataSet(Constants.DICTIONARY_DATASET);
      for (String dataset : datasets) {
        afterDescription = afterDescription.useDataSet(dataset);
      }

      return afterDescription.build();
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
      source = getSource(context);
      source.initialize(context);
      transformation = getTransformation(context);
      transformation.initialize(context);
      sink = getSink(context);
      sink.initialize(context);
    }

    @ProcessInput
    public void process(StreamEvent event) throws Exception {
      Iterator<Record> inputs = source.read(event);
      while (inputs.hasNext()) {
        Record input = inputs.next();
        @Nullable
        Record output = transformation.transform(input);
        if (output != null) {
          sink.write(output);
        }
      }
    }

    @Override
    public void destroy() {
      source.destroy();
      transformation.destroy();
      sink.destroy();
    }

    /**
     * Override it to provide different transformation logic.
     * The default implementation is using program runtime argument
     * {@link com.continuuity.lib.etl.Constants.Realtime.Transformation#ARG_TRANSFORMATION_TYPE} to get the name of
     * a class implementing transformation logic. If this method is not overridden this runtime argument is required
     * @param context instance of {@link com.continuuity.api.flow.flowlet.FlowletContext}
     * @return instance of {@link com.continuuity.lib.etl.transform.Transformation} to be used for transformation
     * @throws Exception
     */
    protected Transformation getTransformation(FlowletContext context) throws Exception {
      String transformationType =
        Programs.getArgOrProperty(context, Constants.Realtime.Transformation.ARG_TRANSFORMATION_TYPE);
      Preconditions.checkArgument(transformationType != null,
                                  "Missing required runtime argument " + Constants.Realtime.Transformation.ARG_TRANSFORMATION_TYPE);

      return (Transformation) Class.forName(transformationType).newInstance();
    }

    /**
     * Override it to provide different source.
     * The default implementation is using program runtime argument {@link com.continuuity.lib.etl.Constants.Realtime.Source#ARG_SOURCE_TYPE}
     * to get the name of a class implementing source. If this method is not overridden this runtime argument is required
     * @param context instance of {@link com.continuuity.api.flow.flowlet.FlowletContext}
     * @return instance of {@link com.continuuity.lib.etl.realtime.source.RealtimeSource} to be used as source
     * @throws Exception
     */
    protected RealtimeSource getSource(FlowletContext context) throws Exception {
      String sourceType = Programs.getArgOrProperty(context, Constants.Realtime.Source.ARG_SOURCE_TYPE);
      Preconditions.checkArgument(sourceType != null,
                                  "Missing required runtime argument " + Constants.Realtime.Source.ARG_SOURCE_TYPE);

      return (RealtimeSource) Class.forName(sourceType).newInstance();
    }

    /**
     * Override it to provide different sink.
     * The default implementation is using program runtime argument {@link com.continuuity.lib.etl.Constants.Realtime.Sink#ARG_SINK_TYPE}
     * to get the name of
     * a class implementing sink. If this method is not overridden this runtime argument is required
     * @param context instance of {@link com.continuuity.api.flow.flowlet.FlowletContext}
     * @return instance of {@link com.continuuity.lib.etl.realtime.sink.RealtimeSink} to be used as sink
     * @throws Exception
     */
    protected RealtimeSink getSink(FlowletContext context) throws Exception {
      String sinkType = Programs.getArgOrProperty(context, Constants.Realtime.Sink.ARG_SINK_TYPE);
      Preconditions.checkArgument(sinkType != null,
                                  "Missing required runtime argument " + Constants.Realtime.Sink.ARG_SINK_TYPE);

      return (RealtimeSink) Class.forName(sinkType).newInstance();
    }


  }
}
