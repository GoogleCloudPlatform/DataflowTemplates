package com.google.cloud.syndeo.it;

import com.google.cloud.storage.Notification;
import com.google.cloud.storage.NotificationInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.syndeo.perf.SyndeoLoadTestUtils;
import com.google.cloud.syndeo.transforms.files.SyndeoFilesReadSchemaTransformProvider;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.it.common.ResourceManager;
import com.google.cloud.teleport.it.common.ResourceManagerUtils;
import com.google.cloud.teleport.it.pubsub.DefaultPubsubResourceManager;
import com.google.cloud.teleport.it.pubsub.PubsubResourceManager;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RunWith(JUnit4.class)
public class ReadFilesTransformIT {

    private static final Logger LOG = LoggerFactory.getLogger(ReadFilesTransformIT.class);

    private static final List<ResourceManager> RESOURCE_MANAGERS = new ArrayList<>();

//    private static final String BUCKET = TestProperties.artifactBucket();
//    private static final String PROJECT = TestProperties.project();
    private static final String TEST_ID = "readfiles-transform-it-" + UUID.randomUUID();

    @Rule
    public final TestName testName = new TestName();

    @Rule
    public final TestPipeline mainPipeline = TestPipeline.create();

    private SubscriptionName pubsubSubscription = null;
    private TopicName pubsubTopic = null;

    @Before
    public void setUpPubSubNotifications() throws IOException {
//        PubsubResourceManager psrm = DefaultPubsubResourceManager.builder(TEST_ID, PROJECT).build();
//        RESOURCE_MANAGERS.add(psrm);
//        String gcsPrefix = Paths.get(BUCKET, TEST_ID, testName.getMethodName()).toString();
//
//        pubsubTopic = psrm.createTopic(TEST_ID + testName.getMethodName());
//        LOG.info("Successfully created topic {}", pubsubTopic);
//        pubsubSubscription = psrm.createSubscription(pubsubTopic, "sub-" + pubsubTopic.getTopic());
//        LOG.info("Successfully created subscription {}", pubsubSubscription);
//
//        Storage storage = StorageOptions.newBuilder().build().getService();
//        NotificationInfo notificationInfo =
//                NotificationInfo.newBuilder(pubsubTopic.getTopic())
//                        .setEventTypes(NotificationInfo.EventType.OBJECT_FINALIZE)
//                        .setObjectNamePrefix(gcsPrefix)
//                        .setPayloadFormat(NotificationInfo.PayloadFormat.JSON_API_V1)
//                        .build();
//        Notification notification = storage.createNotification(BUCKET, notificationInfo);
//        LOG.info("Successfully created notification {}", notification);
    }

    @After
    public void cleanUp() {
        ResourceManagerUtils.cleanResources(RESOURCE_MANAGERS.toArray(new ResourceManager[0]));
        pubsubSubscription = null;
        pubsubTopic = null;
    }

    @Test
    public void testFilesAreConsumed() throws IOException {
        PCollectionRowTuple.empty(mainPipeline)
                .apply(new SyndeoFilesReadSchemaTransformProvider()
                        .from(SyndeoFilesReadSchemaTransformProvider.SyndeoFilesReadSchemaTransformConfiguration.builder()
                                .setFormat("AVRO")
                                .setSchema(AvroUtils.toAvroSchema(SyndeoLoadTestUtils.NESTED_TABLE_SCHEMA).toString())
                                .setPubsubSubscription(pubsubSubscription.toString())
                                .build())
                        .buildTransform()).get("output");

        PipelineResult result = mainPipeline.runWithAdditionalOptionArgs(List.of("--blockOnRun=false"));
        result.waitUntilFinish(Duration.standardSeconds(30));

        result.cancel();

    }

    static void copyFilesToGcs() {
        
    }
}
