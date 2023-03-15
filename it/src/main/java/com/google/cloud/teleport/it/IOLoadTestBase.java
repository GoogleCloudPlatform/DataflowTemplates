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
package com.google.cloud.teleport.it;

import com.google.cloud.teleport.it.dataflow.DirectRunnerClient;
import com.google.cloud.teleport.it.launcher.PipelineLauncher;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for IO Load tests. */
@RunWith(JUnit4.class)
public class IOLoadTestBase extends LoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IOLoadTestBase.class);

  protected String tempBucketName;

  @Before
  public void setUpBase() {
    // Prefer artifactBucket, but use the staging one if none given
    if (TestProperties.hasArtifactBucket()) {
      tempBucketName = TestProperties.artifactBucket();
    } else if (TestProperties.hasStageBucket()) {
      tempBucketName = TestProperties.stageBucket();
    } else {
      LOG.warn(
          "Both -DartifactBucket and -DstageBucket were not given. Pipeline may fail if a temp gcs"
              + " location is needed");
    }
  }

  @Override
  PipelineLauncher launcher() {
    // TODO: the return value is a placeholder currently. Return a pipeline launcher object once
    // there is a generic, non-template launcher available.
    LOG.warn("launcher not fully supported yet.");
    // DirectRunnerClient currently requires a template class as argument. Pass the test class for
    // now.
    return DirectRunnerClient.builder(this.getClass()).setCredentials(CREDENTIALS).build();
  }
}
