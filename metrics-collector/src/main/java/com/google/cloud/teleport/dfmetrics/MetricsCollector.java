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
package com.google.cloud.teleport.dfmetrics;

import com.beust.jcommander.JCommander;
import com.google.cloud.teleport.dfmetrics.commands.Command;
import com.google.cloud.teleport.dfmetrics.commands.CommandFactory;
import com.google.cloud.teleport.dfmetrics.model.CommandLineArgs;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsCollector {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsCollector.class);

  public static void main(String[] argv) throws IllegalAccessException, IOException {

    CommandLineArgs args = new CommandLineArgs();
    JCommander.newBuilder().addObject(args).build().parse(argv);

    LOG.info(
        "Supplied parameters: Command:{}, Config Path:{}, Output Type:{}, Output Location:{}",
        args.command,
        args.configFile,
        args.outputType,
        args.outputLocation);

    Command command = CommandFactory.create(args);
    command.initialize();
    command.run();
  }
}
