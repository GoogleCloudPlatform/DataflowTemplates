package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.templates.StreamingDataGenerator.SinkType;
import com.google.cloud.teleport.v2.testing.dataflow.DataflowOperation;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateClient;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateClient.JobInfo;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateClient.Options;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateSdkClient;
import java.io.IOException;
import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public final class StreamingDataGeneratorIT {

  private static final Duration MAX_WAIT_TIME = Duration.ofMinutes(15);

  private static final String NUM_SHARDS_KEY = "numShards";
  private static final String OUTPUT_DIRECTORY_KEY = "outputDirectory";
  private static final String QPS_KEY = "qps";
  private static final String SCHEMA_LOCATION_KEY = "schemaLocation";
  private static final String SINK_TYPE_KEY = "sinkType";
  private static final String WINDOW_DURATION_KEY = "windowDuration";

  private static final String DEFAULT_QPS = "15";
  private static final String DEFAULT_WINDOW_DURATION = "60s";

  @Test
  public void testFakeMessagesToGcs() throws IOException {
    String name = "teleport_flex_streaming_data_generator_gcs";
    Options options = new Options(name, "TODO")
        .setIsStreaming(true)
        .addParameter(SCHEMA_LOCATION_KEY, "TODO")
        .addParameter(QPS_KEY, DEFAULT_QPS)
        .addParameter(SINK_TYPE_KEY, SinkType.GCS.name())
        .addParameter(WINDOW_DURATION_KEY, DEFAULT_WINDOW_DURATION)
        .addParameter(OUTPUT_DIRECTORY_KEY, "TODO")
        .addParameter(NUM_SHARDS_KEY, "1");
    FlexTemplateClient dataflow = FlexTemplateSdkClient.builder().build();

    JobInfo info = dataflow.launchNewJob("TODO", "TODO", options);
    DataflowOperation.waitForConditionAndFinish(dataflow, null, () -> false);
  }
}
