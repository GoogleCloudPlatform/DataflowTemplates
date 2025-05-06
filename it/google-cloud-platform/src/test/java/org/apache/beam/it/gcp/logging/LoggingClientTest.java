package org.apache.beam.it.gcp.logging;

import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.util.List;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LoggingClientTest {
  PubsubResourceManager pubsubResourceManager;

  @Before
  public void setup() throws IOException {
    pubsubResourceManager =
        PubsubResourceManager.builder("testName", "cloud-teleport-testing", null).build();
  }

  @After
  public void cleanup() {
    pubsubResourceManager.cleanupAll();
  }

  @Test
  public void test() {
    LoggingClient loggingClient =
        LoggingClient.builder(null).setProjectId("cloud-teleport-testing").build();

    List<Payload> logs =
        loggingClient.readLogs(
            "resource.type=\"dataflow_step\"\n"
                + "resource.labels.job_id=\"2025-04-29_02_46_54-9214107254112717404\"\n"
                + "logName=(\"projects/cloud-teleport-testing/logs/dataflow.googleapis.com%2Fjob-message\" OR \"projects/cloud-teleport-testing/logs/dataflow.googleapis.com%2Flauncher\")\n"
                + "\"NOT_FOUND: Unable to find subscription projects/project/subscriptions/IncorrectFormatPubSubSubscriptionID\"",
            10);
    System.out.println(logs);
  }

  @Test
  public void test1() {
    LoggingClient loggingClient =
        LoggingClient.builder(null).setProjectId("cloud-teleport-testing").build();
    String filter = " \"NOT_FOUND: Unable to find subscription\" ";
    List<Payload> logs =
        loggingClient.readJobLogs(
            "2025-04-29_02_46_54-9214107254112717404", filter, Severity.ERROR, 2);
    System.out.println(logs.size());
    System.out.println(logs);
  }

  @Test
  public void testpubsub() throws IOException, InterruptedException {
    TopicName topic = pubsubResourceManager.createTopic("topicNameSuffix");
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, "subscriptionNameSuffix");
    PullResponse pulledMsgs;
    while (true) {
      pulledMsgs = pubsubResourceManager.pull(subscription, 2);
      if (pulledMsgs.getReceivedMessagesCount() > 0) {
        break;
      }
      Thread.sleep(2000);
    }
    System.out.println(pulledMsgs);
    pubsubResourceManager.publish(
        topic,
        pulledMsgs.getReceivedMessages(0).getMessage().getAttributesMap(),
        pulledMsgs.getReceivedMessages(0).getMessage().getData());
    System.out.println(subscription);
  }
}
