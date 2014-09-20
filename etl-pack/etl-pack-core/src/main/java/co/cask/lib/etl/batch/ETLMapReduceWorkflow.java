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

package co.cask.lib.etl.batch;

import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.schedule.Schedule;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowSpecification;

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
