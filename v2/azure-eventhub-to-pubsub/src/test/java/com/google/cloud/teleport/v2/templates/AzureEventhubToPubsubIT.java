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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatPipeline;
import static com.google.cloud.teleport.it.common.matchers.TemplateAsserts.assertThatResult;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.gcp.TemplateTestBase;
import com.google.cloud.teleport.it.gcp.pubsub.PubsubResourceManager;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.utils.SecretManagerUtils;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link AzureEventhubToPubsub} (AzureEventhub_to_Pubsub). Its important to
 * note that Azure Eventhub Namespace and Evethub should be created before starting this pipeline.
 *
 * <p># Parameter required within code eventHubNameSpaceURL: EventHub name space URL of the form-
 * mynamespace.servicebus.windows.net:9093 secret: Secret name which stores the connection string to
 * the Eventhub namespace. example- projects/somenumber/secrets/azurekey/versions/1 eventHubName:
 * Name of Eventhub (or topic) from which data needs to be extracted
 *
 * <p>Example Usage:
 *
 * <pre>
 * # Set the pipeline vars
 * export PROJECT=&lt;project id&gt;
 * export REGION=&lt;dataflow region&gt;
 * export TEMPLATE_MODULE=v2/azure-eventhub-to-pubsub
 * export ARTIFACT_BUCKET=&lt;bucket name&gt;
 * export HOST_IP=&lt;your host ip&gt;
 *
 * # To set the host ip to the default external ip
 * export HOST_IP=$(hostname -I | awk '{print $1}')
 *
 * # To set the gcloud project credential
 * gcloud config set project ${PROJECT}
 * DT_IT_ACCESS_TOKEN=$(gcloud auth application-default print-access-token)
 *
 * # Build and run integration test
 * mvn verify
 *   -pl v2/azure-eventhub-to-pubsub \
 *   -am \
 *   -Dtest="AzureEventhubToPubsubIT" \
 *   -Dproject=${PROJECT} \
 *   -Dregion=${REGION} \
 *   -DartifactBucket=${ARTIFACT_BUCKET} \
 *   -DhostIp=${HOST_IP} \
 *   -Djib.skip \
 *   -DfailIfNoTests=false
 * </pre>
 */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(AzureEventhubToPubsub.class)
@RunWith(JUnit4.class)
public class AzureEventhubToPubsubIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AzureEventhubToPubsubIT.class);

  private PubsubResourceManager pubsubClient;
  private EventHubProducerClient eventHubProducerClient;
  private static final String eventHubNameSpaceURL = "GIVE_YOUR_EVENTHUB_URL_NAME";
  private static final String secret = "GIVE_YOUR_SECRET_NAME";
  private static final String eventHubName = "GIVE_INPUT_EVENTHUBNAME";

  @Before
  public void setup() throws IOException {
    String connectionString = SecretManagerUtils.getSecret(secret);
    eventHubProducerClient =
        new EventHubClientBuilder()
            .connectionString(connectionString, eventHubName)
            .buildProducerClient();
    pubsubClient =
        PubsubResourceManager.builder(testName, PROJECT)
            .credentialsProvider(credentialsProvider)
            .build();
  }

  @After
  public void tearDownClass() {
    boolean producedError = false;

    try {
      pubsubClient.cleanupAll();
    } catch (Exception e) {
      LOG.error("Failed to delete PubSub resources.", e);
      producedError = true;
    }

    try {
      eventHubProducerClient.close();
    } catch (Exception e) {
      LOG.error("Failed to Close connection with Azure EventHub", e);
      producedError = true;
    }

    if (producedError) {
      throw new IllegalStateException("Failed to delete resources. Check above for errors.");
    }
  }

  @Test
  public void testAzureEventhubToPubsub() throws IOException {
    String jobName = testName;
    String psTopic = testName + "output";
    TopicName topicName = pubsubClient.createTopic(psTopic);
    SubscriptionName subscriptionName = pubsubClient.createSubscription(topicName, "subscription");
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath)
            .addParameter("brokerServer", eventHubNameSpaceURL)
            .addParameter("inputTopic", eventHubName)
            .addParameter("outputTopic", topicName.toString())
            .addParameter("secret", secret);
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(info),
                () -> {
                  EventDataBatch eventDataBatch = eventHubProducerClient.createBatch();
                  eventDataBatch.tryAdd(new EventData("Foo"));
                  eventHubProducerClient.send(eventDataBatch);
                  return pubsubClient
                      .pull(subscriptionName, 1)
                      .getReceivedMessages(0)
                      .getMessage()
                      .getData()
                      .toString(StandardCharsets.UTF_8)
                      .equalsIgnoreCase("Foo");
                });
    // Assert
    assertThatResult(result).meetsConditions();
  }
}
