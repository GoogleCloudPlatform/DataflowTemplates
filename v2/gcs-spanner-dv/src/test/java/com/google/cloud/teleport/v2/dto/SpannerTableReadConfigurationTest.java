package com.google.cloud.teleport.v2.dto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SpannerTableReadConfigurationTest {

  @Test
  public void testBuilder() {
    String tableName = "test_table";
    List<String> columnsToInclude = Arrays.asList("col1", "col2");
    List<String> columnsToExclude = Arrays.asList("col3");
    String customQuery = "SELECT * FROM test_table";

    SpannerTableReadConfiguration config = SpannerTableReadConfiguration.builder()
        .setTableName(tableName)
        .setColumnsToInclude(columnsToInclude)
        .setColumnsToExclude(columnsToExclude)
        .setCustomQuery(customQuery)
        .build();

    assertEquals(tableName, config.getTableName());
    assertEquals(columnsToInclude, config.getColumnsToInclude());
    assertEquals(columnsToExclude, config.getColumnsToExclude());
    assertEquals(customQuery, config.getCustomQuery());
  }

  @Test
  public void testBuilder_Nulls() {
    String tableName = "test_table";

    SpannerTableReadConfiguration config = SpannerTableReadConfiguration.builder()
        .setTableName(tableName)
        .build();

    assertEquals(tableName, config.getTableName());
    assertNull(config.getColumnsToInclude());
    assertNull(config.getColumnsToExclude());
    assertNull(config.getCustomQuery());
  }

  @Test
  public void testSerialization() throws Exception {
    SpannerTableReadConfiguration config = SpannerTableReadConfiguration.builder()
        .setTableName("test_table")
        .setColumnsToInclude(List.of("col1"))
        .build();

    SpannerTableReadConfiguration cloned = CoderUtils.clone(
        SchemaCoder.of(
            Objects.requireNonNull(new AutoValueSchema()
                .schemaFor(TypeDescriptor.of(SpannerTableReadConfiguration.class))),
            TypeDescriptor.of(SpannerTableReadConfiguration.class),
            new AutoValueSchema()
                .toRowFunction(TypeDescriptor.of(SpannerTableReadConfiguration.class)),
            new AutoValueSchema()
                .fromRowFunction(TypeDescriptor.of(SpannerTableReadConfiguration.class))),
        config);

    assertEquals(config, cloned);
    Assert.assertNotNull(cloned);
    assertEquals(config.getTableName(), cloned.getTableName());
  }
}
