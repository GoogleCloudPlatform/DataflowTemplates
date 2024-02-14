package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.Type;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.ModColumnType;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerTable;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SchemaUpdateUtils;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.SpannerChangeStreamsUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class SchemaUpdateUtilsTest {
  private static final String changeStreamName = "Singers";
  @Mock private DatabaseClient mockDatabaseClient;
  @Mock private ReadContext mockReadContext;
  @Mock private SpannerAccessor mockSpannerAccessor;
  private Timestamp now = Timestamp.now();

  @Before
  public void setUp() {
    when(mockSpannerAccessor.getDatabaseClient()).thenReturn(mockDatabaseClient);
  }

  public Map<String, TrackedSpannerTable> createSpannerTableByName() {
    List<TrackedSpannerColumn> singersPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("SingerId", Type.int64(), -1, 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("FirstName", Type.string(), 2, -1));
    Map<String, TrackedSpannerTable> spannerTableByName = new HashMap<>();
    spannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    return spannerTableByName;
  }

  @Test
  public void testDetectDiffColumnInModWithoutDiff() {
    ObjectNode pkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    pkColJsonNode.put("SingerId", 1);
    ObjectNode nonPkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    nonPkColJsonNode.put("FirstName", "firstName");
    List<ModColumnType> rowTypes = new ArrayList<>();
    rowTypes.add(new ModColumnType("SingerId", new TypeCode("INT64"), true, 1));
    rowTypes.add(new ModColumnType("FirstName", new TypeCode("STRING"), false, 2));
    Mod mod =
        new Mod(
            pkColJsonNode.toString(),
            nonPkColJsonNode.toString(),
            Timestamp.ofTimeSecondsAndNanos(1650908264L, 925679000),
            "1",
            true,
            "00000001",
            "Singers",
            rowTypes,
            ModType.INSERT,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            1L,
            1L);
    Map<String, TrackedSpannerTable> spannerTableByName = createSpannerTableByName();
    assertThat(SchemaUpdateUtils.detectDiffColumnInMod(mod, spannerTableByName)).isEqualTo(false);
  }

  @Test
  public void testDetectDiffColumnInModWithColDiff() {
    ObjectNode pkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    pkColJsonNode.put("SingerId", 1);
    ObjectNode nonPkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    nonPkColJsonNode.put("FirstName", "firstName");
    nonPkColJsonNode.put("LastName", "lastName");
    List<ModColumnType> rowTypes = new ArrayList<>();
    rowTypes.add(new ModColumnType("SingerId", new TypeCode("{\"code\":\"INT64\"}"), true, 1));
    rowTypes.add(new ModColumnType("FirstName", new TypeCode("{\"code\":\"STRING\"}"), false, 2));
    rowTypes.add(new ModColumnType("LastName", new TypeCode("{\"code\":\"STRING\"}"), false, 3));
    Mod mod =
        new Mod(
            pkColJsonNode.toString(),
            nonPkColJsonNode.toString(),
            Timestamp.ofTimeSecondsAndNanos(1650908264L, 925679000),
            "1",
            true,
            "00000001",
            "Singers",
            rowTypes,
            ModType.INSERT,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            1L,
            1L);
    Map<String, TrackedSpannerTable> spannerTableByName = createSpannerTableByName();
    assertThat(SchemaUpdateUtils.detectDiffColumnInMod(mod, spannerTableByName)).isEqualTo(true);
  }

  @Test
  public void testUpdateStoredSchemaNewRow() {
    ObjectNode pkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    pkColJsonNode.put("SingerId", 1);
    ObjectNode nonPkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    nonPkColJsonNode.put("FirstName", "firstName");
    nonPkColJsonNode.put("LastName", "lastName");
    List<ModColumnType> rowTypes = new ArrayList<>();
    rowTypes.add(new ModColumnType("SingerId", new TypeCode("{\"code\":\"INT64\"}"), true, 1));
    rowTypes.add(new ModColumnType("FirstName", new TypeCode("{\"code\":\"STRING\"}"), false, 2));
    rowTypes.add(new ModColumnType("LastName", new TypeCode("{\"code\":\"STRING\"}"), false, 3));
    Mod mod =
        new Mod(
            pkColJsonNode.toString(),
            nonPkColJsonNode.toString(),
            Timestamp.ofTimeSecondsAndNanos(1650908264L, 925679000),
            "1",
            true,
            "00000001",
            "Singers",
            rowTypes,
            ModType.INSERT,
            ValueCaptureType.NEW_ROW,
            1L,
            1L);
    Map<String, TrackedSpannerTable> spannerTableByName = createSpannerTableByName();
    SchemaUpdateUtils.updateStoredSchemaNewRow(
        mod, spannerTableByName, Dialect.GOOGLE_STANDARD_SQL);
    assertThat(spannerTableByName.get("Singers").getNonPkColumns().size()).isEqualTo(2);
  }

  public void testUpdateStoredSchemaInNeeded(ValueCaptureType valueCaptureType, Dialect dialect) {
    // Construct expectedSpannerTableByName got from INFORMATION_SCHEMA
    Map<String, TrackedSpannerTable> expectedSpannerTableByName = new HashMap<>();
    List<TrackedSpannerColumn> singersPkColumns =
        Collections.singletonList(TrackedSpannerColumn.create("SingerId", Type.int64(), -1, 1));
    List<TrackedSpannerColumn> singersNonPkColumns =
        Arrays.asList(
            TrackedSpannerColumn.create("FirstName", Type.string(), 2, -1),
            TrackedSpannerColumn.create("LastName", Type.string(), 3, -1));
    expectedSpannerTableByName.put(
        "Singers", new TrackedSpannerTable("Singers", singersPkColumns, singersNonPkColumns));
    // Construct the current mod.
    ObjectNode pkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    pkColJsonNode.put("SingerId", 1);
    ObjectNode nonPkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    nonPkColJsonNode.put("FirstName", "firstName");
    nonPkColJsonNode.put("LastName", "lastName");
    List<ModColumnType> rowTypes = new ArrayList<>();
    rowTypes.add(new ModColumnType("SingerId", new TypeCode("{\"code\":\"INT64\"}"), true, 1));
    rowTypes.add(new ModColumnType("FirstName", new TypeCode("{\"code\":\"STRING\"}"), false, 2));
    rowTypes.add(new ModColumnType("LastName", new TypeCode("{\"code\":\"STRING\"}"), false, 3));
    Mod mod =
        new Mod(
            pkColJsonNode.toString(),
            nonPkColJsonNode.toString(),
            Timestamp.ofTimeSecondsAndNanos(1650908264L, 925679000),
            "1",
            true,
            "00000001",
            "Singers",
            rowTypes,
            ModType.INSERT,
            valueCaptureType,
            1L,
            1L);

    try (MockedConstruction<SpannerChangeStreamsUtils> mockedSpannerChangeStreamsUtils =
        Mockito.mockConstruction(
            SpannerChangeStreamsUtils.class,
            (mock, context) -> {
              when(mock.getSpannerTableByName()).thenReturn(expectedSpannerTableByName);
            })) {
      // The current stored schema information.
      Map<String, TrackedSpannerTable> currSpannerTableByName = createSpannerTableByName();
      currSpannerTableByName =
          SchemaUpdateUtils.updateStoredSchemaIfNeeded(
              mockSpannerAccessor, changeStreamName, dialect, mod, currSpannerTableByName);
      // Ensure the stored schema is updated with the schema from INFORMATION_SCHEMA.
      assertThat(expectedSpannerTableByName.get("Singers").getNonPkColumns().size()).isEqualTo(2);
      assertThat(currSpannerTableByName.get("Singers").getNonPkColumns().size()).isEqualTo(2);
    }
  }

  @Test
  public void testUpdateStoredSchemaInNeeded() {
    testUpdateStoredSchemaInNeeded(
        ValueCaptureType.OLD_AND_NEW_VALUES, Dialect.GOOGLE_STANDARD_SQL);
    testUpdateStoredSchemaInNeeded(ValueCaptureType.NEW_ROW, Dialect.GOOGLE_STANDARD_SQL);
    testUpdateStoredSchemaInNeeded(ValueCaptureType.OLD_AND_NEW_VALUES, Dialect.POSTGRESQL);
    testUpdateStoredSchemaInNeeded(ValueCaptureType.NEW_ROW, Dialect.POSTGRESQL);
  }
}
