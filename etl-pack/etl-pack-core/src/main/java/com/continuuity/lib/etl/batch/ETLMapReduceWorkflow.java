package com.continuuity.lib.etl.batch;

import com.continuuity.api.mapreduce.MapReduce;
import com.continuuity.api.schedule.Schedule;
import com.continuuity.api.workflow.Workflow;
import com.continuuity.api.workflow.WorkflowSpecification;

/**
 * Schedules MapReduce ETL job
 */
public class ETLMapReduceWorkflow implements Workflow {

  private MapReduce mapReduce = null;

  public ETLMapReduceWorkflow(MapReduce mapReduce) {
    this.mapReduce = mapReduce;
  }

  @Override
  public WorkflowSpecification configure() {
    long intervalInMinutes = 10;
    return WorkflowSpecification.Builder.with()
      .setName("ETLMapReduceWorkflow")
      .setDescription("Performs incremental processing with MapReduce job")
      .onlyWith(mapReduce == null ? new ETLMapReduce() : mapReduce)
      .addSchedule(new Schedule(
        "FiveMinuteSchedule", "Run every " + intervalInMinutes + " minutes",
        "0/" + intervalInMinutes + " * * * *", Schedule.Action.START))
      .build();
  }
}
