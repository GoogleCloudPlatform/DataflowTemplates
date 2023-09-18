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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.cloud.teleport.dfmetrics.model.CommandLineArgs;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CommandFactoryTest {

  @Test
  public void testCommandFactory_withLaunchTemplate_returnsTemplateLaunchCommand()
      throws IOException, IllegalAccessException {
    CommandLineArgs args = new CommandLineArgs();
    args.command = CommandFactory.LAUNCH_AND_COLLECT;
    args.outputType = "file";
    args.outputLocation = "/path/to/metrics.json";

    Command command = CommandFactory.create(args);
    assertThat(command instanceof TemplateLauncherCommand, is(true));
  }

  @Test
  public void testCommandFactory_withCollectMetrics_returnsMetricsFetcherCommand()
      throws IOException, IllegalAccessException {
    CommandLineArgs args = new CommandLineArgs();
    args.command = CommandFactory.COLLECT_METRICS;
    args.outputType = "bigquery";
    args.outputLocation = "projectid.dataset.tablenmae";

    Command command = CommandFactory.create(args);
    assertThat(command instanceof MetricsFetcherCommand, is(true));
  }

  @Test(expected = RuntimeException.class)
  public void testCommandFactory_withInvalidCommand_throwsException() throws Exception {
    CommandLineArgs args = new CommandLineArgs();
    args.command = "UNKNOWN";
    args.outputType = "bigquery";
    args.outputLocation = "projectid.dataset.tablenmae";
    CommandFactory.create(args);
  }
}
