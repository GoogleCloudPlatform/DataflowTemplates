package org.apache.beam.it.gcp.spanner;

import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Base class for all Spanner template integration tests. This class parameterizes the spannerHost
 * for all subclasses. If the "spannerHost" system property is not set, the value of spannerHost
 * will be set to STAGING_SPANNER_HOST and DEFAULT_SPANNER_HOST (as defined in {@link
 * SpannerResourceManager}). All tests in the base class will be run twice: once with spannerHost
 * set to STAGING_SPANNER_HOST and once with spannerHost set to DEFAULT_SPANNER_HOST. Otherwise, If
 * the "spannerHost" system property is set, its value will be used to set spannerHost. All
 * subclasses must use SpannerResourceManager.useCustomHost() and pass the spannerHost parameter to
 * it.
 */
@RunWith(Parameterized.class)
public abstract class SpannerTemplateITBase extends TemplateTestBase {

  @Parameterized.Parameter(0)
  public String spannerHost;

  @Parameterized.Parameter(1)
  public String spannerHostName;

  protected Set<String> stagingEnabledTests() {
    // staging endpoint test disabled in base class. To enable it for certain test,
    // override this in derived test class.
    return ImmutableSet.of();
  }

  @Before
  public void setupSpannerBase() {
    if ("Staging".equals(spannerHostName)) {
      // Only executes allow-listed staging test
      assumeTrue(stagingEnabledTests().contains(testName));
    }
  }

  // Because of parameterization, the test names will have subscripts. For example:
  // testSpannerToGCSAvroBase[Staging]
  @Parameters(name = "{1}")
  public static Collection parameters() {
    if (System.getProperty("spannerHost") != null) {
      return Arrays.asList(new Object[][] {{System.getProperty("spannerHost"), "Custom"}});
    }
    return Arrays.asList(
        new Object[][] {
          {SpannerResourceManager.STAGING_SPANNER_HOST, "Staging"},
          {SpannerResourceManager.DEFAULT_SPANNER_HOST, "Default"}
        });
  }

  public static Map<String, Object> createSessionTemplate(
      int numTables,
      List<Map<String, Object>> columnConfigs,
      List<Map<String, Object>> primaryKeyConfig) {
    Map<String, Object> sessionTemplate = new LinkedHashMap<>();
    sessionTemplate.put("SessionName", "NewSession");
    sessionTemplate.put("EditorName", "");
    sessionTemplate.put("DatabaseType", "mysql");
    sessionTemplate.put("DatabaseName", "SP_DATABASE");
    sessionTemplate.put("Dialect", "google_standard_sql");
    sessionTemplate.put("Notes", null);
    sessionTemplate.put("Tags", null);
    sessionTemplate.put("SpSchema", new LinkedHashMap<>());
    sessionTemplate.put("SyntheticPKeys", new LinkedHashMap<>());
    sessionTemplate.put("SrcSchema", new LinkedHashMap<>());
    sessionTemplate.put("SchemaIssues", new LinkedHashMap<>());
    sessionTemplate.put("Location", new LinkedHashMap<>());
    sessionTemplate.put("TimezoneOffset", "+00:00");
    sessionTemplate.put("SpDialect", "google_standard_sql");
    sessionTemplate.put("UniquePKey", new LinkedHashMap<>());
    sessionTemplate.put("Rules", new ArrayList<>());
    sessionTemplate.put("IsSharded", false);
    sessionTemplate.put("SpRegion", "");
    sessionTemplate.put("ResourceValidation", false);
    sessionTemplate.put("UI", false);

    for (int i = 1; i <= numTables; i++) {
      String tableName = "TABLE" + i;
      List<String> colIds = new ArrayList<>();
      Map<String, Object> colDefs = new LinkedHashMap<>();

      for (int j = 0; j < columnConfigs.size(); j++) {
        Map<String, Object> colConfig = columnConfigs.get(j);
        String colId = (String) colConfig.getOrDefault("id", "c" + (j + 1));
        colIds.add(colId);

        Map<String, Object> colType = new LinkedHashMap<>();
        colType.put("Name", colConfig.getOrDefault("Type", "STRING"));
        colType.put("Len", colConfig.getOrDefault("Length", 0));
        colType.put("IsArray", colConfig.getOrDefault("IsArray", false));

        Map<String, Object> column = new LinkedHashMap<>();
        column.put("Name", colConfig.getOrDefault("Name", "column_" + (j + 1)));
        column.put("T", colType);
        column.put("NotNull", colConfig.getOrDefault("NotNull", false));
        column.put("Comment", colConfig.getOrDefault("Comment", ""));
        column.put("Id", colId);
        colDefs.put(colId, column);
      }

      List<Map<String, Object>> primaryKeys = new ArrayList<>();
      for (Map<String, Object> pk : primaryKeyConfig) {
        Map<String, Object> pkEntry = new LinkedHashMap<>();
        pkEntry.put("ColId", pk.get("ColId"));
        pkEntry.put("Desc", pk.getOrDefault("Desc", false));
        pkEntry.put("Order", pk.getOrDefault("Order", 1));
        primaryKeys.add(pkEntry);
      }

      Map<String, Object> spSchemaEntry = new LinkedHashMap<>();
      spSchemaEntry.put("Name", tableName);
      spSchemaEntry.put("ColIds", colIds);
      spSchemaEntry.put("ShardIdColumn", "");
      spSchemaEntry.put("ColDefs", colDefs);
      spSchemaEntry.put("PrimaryKeys", primaryKeys);
      spSchemaEntry.put("ForeignKeys", null);
      spSchemaEntry.put("Indexes", null);
      spSchemaEntry.put("ParentId", "");
      spSchemaEntry.put("Comment", "Spanner schema for source table " + tableName);
      spSchemaEntry.put("Id", "t" + i);
      ((Map<String, Object>) sessionTemplate.get("SpSchema")).put("t" + i, spSchemaEntry);

      Map<String, Object> srcSchemaEntry = new LinkedHashMap<>(spSchemaEntry);
      srcSchemaEntry.put("Schema", "SRC_DATABASE");
      ((Map<String, Object>) sessionTemplate.get("SrcSchema")).put("t" + i, srcSchemaEntry);

      Map<String, Object> schemaIssuesEntry = new LinkedHashMap<>();
      schemaIssuesEntry.put("ColumnLevelIssues", new LinkedHashMap<>());
      schemaIssuesEntry.put("TableLevelIssues", null);
      ((Map<String, Object>) sessionTemplate.get("SchemaIssues")).put("t" + i, schemaIssuesEntry);
    }

    return sessionTemplate;
  }

  /** Helper function for checking the rows of the destination Spanner tables. */
  public static void checkSpannerTables(
      SpannerResourceManager spannerResourceManager,
      List<String> tableNames,
      Map<String, List<Map<String, Object>>> cdcEvents,
      List<String> cols) {
    tableNames.forEach(
        tableName -> {
          SpannerAsserts.assertThatStructs(spannerResourceManager.readTableRecords(tableName, cols))
              .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName));
        });
  }
}
