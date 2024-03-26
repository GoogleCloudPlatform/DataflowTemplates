package com.google.cloud.teleport.v2.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.coders.JsonNodeCoder;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.constants.Constants;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ColumnPK;
import com.google.cloud.teleport.v2.spanner.migrations.schema.NameAndCols;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnDefinition;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerColumnType;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SyntheticPKey;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JsonNodeToMutationDoFnTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testNodeToMutation_basic() throws Exception {
        Schema schema = getSchemaObject();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode changeEvent =
                mapper.readTree(
                        String.format("{\"%s\":\"cart\", \"product_id\":\"product1\", \"quantity\": 5, \"user_id\": \"user1\"}", Constants.EVENT_TABLE_NAME_KEY));
        List<JsonNode> inputList = ImmutableList.of(changeEvent);
        PCollection<JsonNode> input =
                pipeline.apply(
                        "input",
                        Create.of(inputList)
                                .withCoder(JsonNodeCoder.of()));
        PCollection<Mutation> mutations =
                input.apply(
                        "Map JsonNode to Mutations",
                        ParDo.of(
                                new JsonNodeToMutationDoFn(
                                        schema, "cart", getTestDdl())));
        PAssert.that(mutations).containsInAnyOrder(
                Mutation.newInsertOrUpdateBuilder("new_cart")
                        .set("new_product_id")
                        .to("product1")
                        .set("new_quantity")
                        .to(5)
                        .set("new_user_id")
                        .to("user1")
                        .build());

        pipeline.run();
    }

    @Test
    public void testNodeToMutation_exception() throws Exception {
        Schema schema = getSchemaObject();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode changeEvent =
                mapper.readTree(
                        String.format("{\"%s\":\"cart\",\"quantity\": \"hello\"}", Constants.EVENT_TABLE_NAME_KEY));
        List<JsonNode> inputList = ImmutableList.of(changeEvent);
        PCollection<JsonNode> input =
                pipeline.apply(
                        "input",
                        Create.of(inputList)
                                .withCoder(JsonNodeCoder.of()));
        PCollection<Mutation> mutations =
                input.apply(
                        "Map JsonNode to Mutations",
                        ParDo.of(
                                new JsonNodeToMutationDoFn(
                                        schema, "cart", getTestDdl())));

        PipelineResult result = pipeline.run();
        result.waitUntilFinish();

        Map<String, Long> expectedCounters = new HashMap<>();
        expectedCounters.put("mutationBuildErrors", 1L);
        for (MetricResult c :
                result.metrics().queryMetrics(MetricsFilter.builder().build()).getCounters()) {
            String name = c.getName().getName();
            if (expectedCounters.containsKey(name)) {
                assertEquals(expectedCounters.get(name), c.getCommitted());
                expectedCounters.remove(name);
            }
        }
        assertTrue(expectedCounters.isEmpty());
    }

    private static Ddl getTestDdl() {
        Ddl ddl =
                Ddl.builder()
                        .createTable("new_cart")
                        .column("new_quantity")
                        .int64()
                        .notNull()
                        .endColumn()
                        .column("new_user_id")
                        .string()
                        .max()
                        .endColumn()
                        .column("new_product_id")
                        .string()
                        .max()
                        .endColumn()
                        .primaryKey()
                        .asc("new_user_id")
                        .asc("new_product_id")
                        .end()
                        .endTable()
                        .build();
        return ddl;
    }

    private static Schema getSchemaObject() {
        // Add Synthetic PKs.
        Map<String, SyntheticPKey> syntheticPKeys = getSyntheticPks();
        // Add SrcSchema.
        Map<String, SourceTable> srcSchema = getSampleSrcSchema();
        // Add SpSchema.
        Map<String, SpannerTable> spSchema = getSampleSpSchema();
        // Add ToSpanner.
        Map<String, NameAndCols> toSpanner = getToSpanner();
        // Add SrcToID.
        Map<String, NameAndCols> srcToId = getSrcToId();
        Schema expectedSchema = new Schema(spSchema, syntheticPKeys, srcSchema);
        expectedSchema.setToSpanner(toSpanner);
        expectedSchema.setToSource(new HashMap<String, NameAndCols>());
        expectedSchema.setSrcToID(srcToId);
        expectedSchema.setSpannerToID(new HashMap<String, NameAndCols>());
        expectedSchema.generateMappings();
        return expectedSchema;
    }

    private static Map<String, SyntheticPKey> getSyntheticPks() {
        Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
        syntheticPKeys.put("t2", new SyntheticPKey("c6", 0));
        return syntheticPKeys;
    }

    private static Map<String, SourceTable> getSampleSrcSchema() {
        Map<String, SourceTable> srcSchema = new HashMap<String, SourceTable>();
        Map<String, SourceColumnDefinition> t1SrcColDefs =
                new HashMap<String, SourceColumnDefinition>();
        t1SrcColDefs.put(
                "c1",
                new SourceColumnDefinition(
                        "product_id", new SourceColumnType("varchar", new Long[]{20L}, null)));
        t1SrcColDefs.put(
                "c2", new SourceColumnDefinition("quantity", new SourceColumnType("bigint", null, null)));
        t1SrcColDefs.put(
                "c3",
                new SourceColumnDefinition(
                        "user_id", new SourceColumnType("varchar", new Long[]{20L}, null)));
        srcSchema.put(
                "t1",
                new SourceTable(
                        "cart",
                        "my_schema",
                        new String[]{"c3", "c1", "c2"},
                        t1SrcColDefs,
                        new ColumnPK[]{new ColumnPK("c3", 1), new ColumnPK("c1", 2)}));
        Map<String, SourceColumnDefinition> t2SrcColDefs =
                new HashMap<String, SourceColumnDefinition>();
        t2SrcColDefs.put(
                "c5",
                new SourceColumnDefinition(
                        "name", new SourceColumnType("varchar", new Long[]{20L}, null)));
        srcSchema.put(
                "t2", new SourceTable("people", "my_schema", new String[]{"c5"}, t2SrcColDefs, null));
        return srcSchema;
    }

    private static Map<String, SpannerTable> getSampleSpSchema() {
        Map<String, SpannerTable> spSchema = new HashMap<String, SpannerTable>();
        Map<String, SpannerColumnDefinition> t1SpColDefs =
                new HashMap<String, SpannerColumnDefinition>();
        t1SpColDefs.put(
                "c1",
                new SpannerColumnDefinition("new_product_id", new SpannerColumnType("STRING", false)));
        t1SpColDefs.put(
                "c2", new SpannerColumnDefinition("new_quantity", new SpannerColumnType("INT64", false)));
        t1SpColDefs.put(
                "c3", new SpannerColumnDefinition("new_user_id", new SpannerColumnType("STRING", false)));
        spSchema.put(
                "t1",
                new SpannerTable(
                        "new_cart",
                        new String[]{"c1", "c2", "c3"},
                        t1SpColDefs,
                        new ColumnPK[]{new ColumnPK("c3", 1), new ColumnPK("c1", 2)},
                        ""));
        Map<String, SpannerColumnDefinition> t2SpColDefs =
                new HashMap<String, SpannerColumnDefinition>();
        t2SpColDefs.put(
                "c5", new SpannerColumnDefinition("new_name", new SpannerColumnType("STRING", false)));
        t2SpColDefs.put(
                "c6", new SpannerColumnDefinition("synth_id", new SpannerColumnType("INT64", false)));
        spSchema.put(
                "t2",
                new SpannerTable(
                        "new_people",
                        new String[]{"c5", "c6"},
                        t2SpColDefs,
                        new ColumnPK[]{new ColumnPK("c6", 1)},
                        ""));
        return spSchema;
    }

    private static Map<String, NameAndCols> getToSpanner() {
        Map<String, NameAndCols> toSpanner = new HashMap<String, NameAndCols>();
        Map<String, String> t1Cols = new HashMap<String, String>();
        t1Cols.put("product_id", "new_product_id");
        t1Cols.put("quantity", "new_quantity");
        t1Cols.put("user_id", "new_user_id");
        toSpanner.put("cart", new NameAndCols("new_cart", t1Cols));
        Map<String, String> t2Cols = new HashMap<String, String>();
        t2Cols.put("name", "new_name");
        toSpanner.put("people", new NameAndCols("new_people", t2Cols));
        return toSpanner;
    }

    private static Map<String, NameAndCols> getSrcToId() {
        Map<String, NameAndCols> srcToId = new HashMap<String, NameAndCols>();
        Map<String, String> t1ColIds = new HashMap<String, String>();
        t1ColIds.put("product_id", "c1");
        t1ColIds.put("quantity", "c2");
        t1ColIds.put("user_id", "c3");
        srcToId.put("cart", new NameAndCols("t1", t1ColIds));
        Map<String, String> t2ColIds = new HashMap<String, String>();
        t2ColIds.put("name", "c5");
        srcToId.put("people", new NameAndCols("t2", t2ColIds));
        return srcToId;
    }
}
