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
        "-pl metadata,v2/common,v2/spanner-change-streams-to-sharded-file-sink -am -PtemplatesStage,pluginOutputDir,splunkDeps,missing-artifact-repos -DskipShade=true -DskipTests -Dmaven.test.skip -Dcheckstyle.skip -Dmdep.analyze.skip -Dspotless.check.skip -Denforcer.skip -DprojectId=testProject -Dregion=us-central1 -DbucketName=TestBucketName -DgcpTempLocation=TestBucketName -DstagePrefix=TestClassName -DtemplateName=Spanner_Change_Streams_to_Sharded_File_Sink -DunifiedWorker=true -e";
    assertThat(String.join(" ", Arrays.copyOfRange(actual, 6, 25))).isEqualTo(expected);
    assertThat(actual[5]).endsWith("pom.xml");
    assertThat(actual[25]).isNotEmpty();
  }
}
