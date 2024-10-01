/*
 * Copyright (C) 2024 Google LLC
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
package org.apache.beam.it.gcp.dataflow;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit test for {@link FlexTemplateDataflowJobResourceManager}. */
@RunWith(JUnit4.class)
public class FlexTemplateDataflowJobResourceManagerTest {

  @Test
  public void testBuildMavenStageCommand() {
    System.setProperty("project", "testProject");
    System.setProperty("region", "us-central1");
    System.setProperty("unifiedWorker", "true");
    System.setProperty("stageBucket", "testStageBucket");

    FlexTemplateDataflowJobResourceManager manager =
        mock(FlexTemplateDataflowJobResourceManager.class);
    when(manager.buildMavenStageCommand(any(), any(), any(), any())).thenCallRealMethod();

    String[] actual =
        manager.buildMavenStageCommand(
            "TestClassName",
            "TestBucketName",
            "Spanner_Change_Streams_to_Sharded_File_Sink",
            "v2/spanner-change-streams-to-sharded-file-sink");
    assertThat(String.join(" ", Arrays.copyOfRange(actual, 0, 5)))
        .isEqualTo("mvn compile package -q -f");
    String expected =
        "-pl metadata,v2/common,v2/spanner-change-streams-to-sharded-file-sink -am -PtemplatesStage,pluginOutputDir -DskipShade=true -DskipTests -Dmaven.test.skip -Dcheckstyle.skip -Dmdep.analyze.skip -Dspotless.check.skip -Denforcer.skip -DprojectId=testProject -Dregion=us-central1 -DbucketName=TestBucketName -DgcpTempLocation=TestBucketName -DstagePrefix=TestClassName -DtemplateName=Spanner_Change_Streams_to_Sharded_File_Sink -DunifiedWorker=true -e";
    assertThat(String.join(" ", Arrays.copyOfRange(actual, 6, 25))).isEqualTo(expected);
    assertThat(actual[5]).endsWith("pom.xml");
    assertThat(actual[25]).isNotEmpty();
  }
}
