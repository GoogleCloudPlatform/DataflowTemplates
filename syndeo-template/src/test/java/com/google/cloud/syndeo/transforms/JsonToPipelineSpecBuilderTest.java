package com.google.cloud.syndeo.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.syndeo.SyndeoTemplate;
import com.google.cloud.syndeo.common.ProviderUtil;
import com.google.cloud.syndeo.v1.SyndeoV1;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class JsonToPipelineSpecBuilderTest {
    public static final Logger LOG = LoggerFactory.getLogger(JsonToPipelineSpecBuilderTest.class);

    @Parameterized.Parameters
    public static List<JsonNode> loadSampleConfigs() {
        try {
            InputStream is = Resources.getResource("json_spec_payload_sample.json").openStream();
            String sampleData = new String(is.readAllBytes(), StandardCharsets.UTF_8);

            ObjectMapper om = new ObjectMapper();
            JsonNode samples = om.readTree(sampleData);
            return Lists.newArrayList(samples.iterator());
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize sample configs. ", e);
        }
    }

    private Boolean canTestBuildPipeline() {
        return !sample.toString().contains("confluentSchemaRegistryUrl");
    }

    @Parameterized.Parameter
    public JsonNode sample;

    @Test
    public void testPipelinesCanBeBuiltWithJsonSampleSpecs() throws IOException {
        SyndeoV1.PipelineDescription desc = SyndeoTemplate.buildFromJsonPayload(sample.toString());
        List<ProviderUtil.TransformSpec> specs = new ArrayList<>();
        for (SyndeoV1.ConfiguredSchemaTransform inst : desc.getTransformsList()) {
            specs.add(new ProviderUtil.TransformSpec(inst));
        }

        if (canTestBuildPipeline()) {
            Pipeline p = Pipeline.create();
            // Run pipeline from configuration.
            ProviderUtil.applyConfigs(specs, PCollectionRowTuple.empty(p));
        }
    }
}
