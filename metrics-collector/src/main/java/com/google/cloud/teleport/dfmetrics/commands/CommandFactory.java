/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.dfmetrics.commands;

import com.google.api.services.dataflow.Dataflow;
import com.google.cloud.teleport.dfmetrics.model.CommandLineArgs;
import com.google.cloud.teleport.dfmetrics.model.DataflowClient;
import com.google.cloud.teleport.dfmetrics.output.IOutputStore;
import com.google.cloud.teleport.dfmetrics.output.OutputStoreFactory;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Creates instance of {@link Command}. */
public class CommandFactory {
  private static final Logger LOG = LoggerFactory.getLogger(CommandFactory.class);

  static final String COLLECT_METRICS = "COLLECT_METRICS";
  static final String LAUNCH_AND_COLLECT = "LAUNCH_AND_COLLECT";

  private CommandFactory() {}

  /** Creates appropriate command instance based on user supplied command type. */
  public static Command create(CommandLineArgs args) throws IOException, IllegalAccessException {
    IOutputStore outputStore = OutputStoreFactory.create(args.outputType, args.outputLocation);
    Dataflow dataflowClient = DataflowClient.builder().build();

    switch (args.command.toUpperCase()) {
      case COLLECT_METRICS:
        LOG.info("Executing Metrics Fetcher command");
        return new MetricsFetcherCommand(args, dataflowClient, outputStore);
      case LAUNCH_AND_COLLECT:
        LOG.info("Executing Template launcher command");
        return new TemplateLauncherCommand(args, dataflowClient, outputStore);
      default:
        throw new IllegalArgumentException(
            "Invalid output type. Supported commands are: LAUNCHTEMPLATE, COLLECTMETRICS");
    }
  }
}
