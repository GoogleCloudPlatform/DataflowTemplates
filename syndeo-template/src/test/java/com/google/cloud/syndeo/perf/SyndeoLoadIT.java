package com.google.cloud.syndeo.perf;

import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformConfiguration;
import com.google.cloud.syndeo.transforms.bigtable.BigTableWriteSchemaTransformProvider;
import com.google.cloud.teleport.it.TestProperties;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteWriteSchemaTransformProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.Random;

@Category(TemplateIntegrationTest.class)
@RunWith(JUnit4.class)
public class SyndeoLoadIT {
    @Rule
    public TestPipeline dataGenerator = TestPipeline.create();

    @Rule
    public TestPipeline syndeoPipeline = TestPipeline.create();

    private static final String PROJECT = TestProperties.project();
    private static final String LOCATION = TestProperties.region();
    private static final String BIGTABLE_INSTANCE = TestProperties.getProperty("bigTable", "syndeo-bigtable", TestProperties.Type.PROPERTY);

    private static final

    @Test
    public void testPubsubLiteToBigTableSyndeoFlow () throws NoSuchSchemaException {
        long numRows = 1_000_000;
        String testId = SyndeoLoadTestUtils.randomString(20, new Random());
        String topicName = "pubsublite_syndeo_test_" + testId;
        String subscriptionName = "subscription__" + topicName;

        String bigTableName = "bigtable_syndeo_test_" + testId;

        PCollectionRowTuple.of("input", SyndeoLoadTestUtils.inputData(dataGenerator, numRows))
                .apply(new PubsubLiteWriteSchemaTransformProvider()
                        .from(
                                // TODO(pabloem): Avoid relying on SchemaRegistry and make from(ConfigClass) public.
                                SchemaRegistry.createDefault().getToRowFunction(PubsubLiteWriteSchemaTransformProvider.PubsubLiteWriteSchemaTransformConfiguration.class).apply(
                                PubsubLiteWriteSchemaTransformProvider.PubsubLiteWriteSchemaTransformConfiguration.builder()
                                .setFormat("AVRO")
                                .setTopicName(topicName)
                                .setLocation(LOCATION)
                                .setProject(PROJECT)
                                .build()))
                        .buildTransform());
        PipelineResult generatorResult = dataGenerator.run();

        PCollectionRowTuple.empty(syndeoPipeline)
                .apply(new PubsubLiteReadSchemaTransformProvider().from(
                        PubsubLiteReadSchemaTransformProvider.PubsubLiteReadSchemaTransformConfiguration.builder()
                                .setLocation(LOCATION)
                                .setProject(PROJECT)
                                .setDataFormat("AVRO")
                                .setSchema(AvroUtils.toAvroSchema(SyndeoLoadTestUtils.SIMPLE_TABLE_SCHEMA).toString())
                                .setSubscriptionName(subscriptionName)
                                .build()
                ).buildTransform())
                .apply(new BigTableWriteSchemaTransformProvider().from(BigTableWriteSchemaTransformConfiguration.builder()
                        .setProjectId(PROJECT)
                                .setTableId(bigTableName)
                                .setInstanceId(BIGTABLE_INSTANCE)
                                .setKeyColumns(Collections.singletonList("sha1"))   // The sha1 of a commit is a byte array.
                        .build()).buildTransform());
        PipelineResult syndeoResult = syndeoPipeline.run();

        generatorResult.waitUntilFinish();
        syndeoResult.waitUntilFinish();
    }
}
