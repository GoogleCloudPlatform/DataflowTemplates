/*
 * Copyright (C) 2022 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaPartitionField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaSchemaField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1StorageFormat;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import com.google.cloud.teleport.v2.options.DataplexJdbcIngestionOptions;
import com.google.cloud.teleport.v2.utils.DataplexJdbcPartitionUtils.PartitioningSchema;
import com.google.cloud.teleport.v2.utils.DataplexUtils;
import com.google.cloud.teleport.v2.values.DataplexEnums.CompressionFormat;
import com.google.cloud.teleport.v2.values.DataplexEnums.EntityType;
import com.google.cloud.teleport.v2.values.DataplexEnums.PartitionStyle;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageFormat;
import com.google.cloud.teleport.v2.values.DataplexEnums.StorageSystem;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link DataplexBigQueryToGcs}. */
@RunWith(JUnit4.class)
public class DataplexJdbcIngestionTest {

  private static final String PROJECT = "test-project1";
  private static final String ASSET_NAME =
      "projects/test-project1/locations/us-central1/lakes/lake1/zones/zone1/assets/asset1";
  private static final String ZONE_NAME = DataplexUtils.getZoneFromAsset(ASSET_NAME);

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Rule public final ExpectedException exceptionRule = ExpectedException.none();
  @Mock private DataplexClient dataplexClientMock;

  private DataplexJdbcIngestionOptions options;
  private Schema avroSchema;
  private GoogleCloudDataplexV1Schema dataplexUnpartitionedSchema;

  @Before
  public void setUp() throws InterruptedException, IOException {
    options = TestPipeline.testingPipelineOptions().as(DataplexJdbcIngestionOptions.class);
    options.setProject(PROJECT);
    options.setOutputAsset(ASSET_NAME);

    avroSchema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":"
                    + "[{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},"
                    + "{\"name\":\"s1\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"d1\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]},"
                    + "{\"name\":\"t1\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}}]}");
    dataplexUnpartitionedSchema =
        new GoogleCloudDataplexV1Schema()
            .setFields(
                Arrays.asList(
                    dataplexField("ts", "TIMESTAMP", "NULLABLE"),
                    dataplexField("s1", "STRING", "NULLABLE"),
                    dataplexField("d1", "DATE", "NULLABLE"),
                    dataplexField("t1", "TIME", "REQUIRED")));

    when(dataplexClientMock.createEntity(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Entity entity = invocation.getArgument(1);
              return entity.clone().setName("generated_name-" + entity.getId());
            });
    when(dataplexClientMock.updateEntity(any()))
        .thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  public void test_loadDataplexMetadata_createsNewUnpartitionedEntity() throws IOException {
    options.setUpdateDataplexMetadata(true);
    options.setOutputTable("table_foo");

    when(dataplexClientMock.listEntities(any(), any())).thenReturn(ImmutableList.of());

    GoogleCloudDataplexV1Entity actualEntity =
        DataplexJdbcIngestion.loadDataplexMetadata(
            dataplexClientMock, options, avroSchema, "gs://target_bucket/table_foo", false);

    GoogleCloudDataplexV1Entity expectedEntity =
        defaultDataplexEntity()
            .setId("table_foo")
            .setName("generated_name-table_foo")
            .setDataPath("gs://target_bucket/table_foo")
            .setSchema(dataplexUnpartitionedSchema.clone().setUserManaged(true));

    assertThat(actualEntity).isEqualTo(expectedEntity);
    verify(dataplexClientMock, times(1))
        .createEntity(ZONE_NAME, expectedEntity.clone().setName(null));
    // Make sure no other entities were created:
    verify(dataplexClientMock, times(1)).createEntity(any(), any());
  }

  @Test
  public void test_loadDataplexMetadata_createsNewPartitionedEntity() throws IOException {
    options.setUpdateDataplexMetadata(true);
    options.setOutputTable("table_foo");
    options.setPartitioningScheme(PartitioningSchema.DAILY);
    options.setParitionColumn("ts");

    when(dataplexClientMock.listEntities(any(), any())).thenReturn(ImmutableList.of());

    GoogleCloudDataplexV1Entity actualEntity =
        DataplexJdbcIngestion.loadDataplexMetadata(
            dataplexClientMock, options, avroSchema, "gs://target_bucket/table_foo", true);

    GoogleCloudDataplexV1Entity expectedEntity =
        defaultDataplexEntity()
            .setId("table_foo")
            .setName("generated_name-table_foo")
            .setDataPath("gs://target_bucket/table_foo")
            .setSchema(
                dataplexUnpartitionedSchema
                    .clone()
                    .setUserManaged(true)
                    .setPartitionStyle(PartitionStyle.HIVE_COMPATIBLE.name())
                    .setPartitionFields(
                        Arrays.asList(
                            dataplexPartitionField("year", "STRING"),
                            dataplexPartitionField("month", "STRING"),
                            dataplexPartitionField("day", "STRING"))));

    assertThat(actualEntity).isEqualTo(expectedEntity);
    verify(dataplexClientMock, times(1))
        .createEntity(ZONE_NAME, expectedEntity.clone().setName(null));
    // Make sure no other entities were created:
    verify(dataplexClientMock, times(1)).createEntity(any(), any());
  }

  @Test
  public void test_loadDataplexMetadata_updatesExistingEntity() throws IOException {
    options.setUpdateDataplexMetadata(true);
    options.setOutputTable("table_foo");

    GoogleCloudDataplexV1Entity existingEntity =
        defaultDataplexEntity()
            .setName("table_foo-name")
            .setId("table_foo")
            .setDataPath("gs://target_bucket/table_foo")
            // Check that if the existing entity has different schema, the schema gets updated:
            .setSchema(
                new GoogleCloudDataplexV1Schema()
                    .setFields(Arrays.asList(dataplexField("foobar", "STRING", "REQUIRED")))
                    .setUserManaged(true));
    when(dataplexClientMock.listEntities(eq(ZONE_NAME), any()))
        .thenAnswer(
            invocation -> {
              String filter = invocation.getArgument(1);
              if (filter.equals("data_path=\"gs://target_bucket/table_foo\"")) {
                // listEntities Dataplex API method doesn't return schema, only getEntity does.
                return ImmutableList.of(existingEntity.clone().setSchema(null));
              } else {
                return ImmutableList.of();
              }
            });
    when(dataplexClientMock.getEntity("table_foo-name")).thenReturn(existingEntity);

    GoogleCloudDataplexV1Entity actualEntity =
        DataplexJdbcIngestion.loadDataplexMetadata(
            dataplexClientMock, options, avroSchema, "gs://target_bucket/table_foo", false);

    GoogleCloudDataplexV1Entity expectedEntity =
        existingEntity.clone().setSchema(dataplexUnpartitionedSchema.clone().setUserManaged(true));

    assertThat(actualEntity).isEqualTo(expectedEntity);
    verify(dataplexClientMock, times(1)).updateEntity(expectedEntity);
    // Make sure no other entities were updated:
    verify(dataplexClientMock, times(1)).updateEntity(any());
  }

  @Test
  public void test_loadDataplexMetadata_throwsExceptionIfExistingEntityNotUserManaged()
      throws IOException {
    options.setUpdateDataplexMetadata(true);
    options.setOutputTable("table_foo");

    GoogleCloudDataplexV1Entity existingEntity =
        defaultDataplexEntity()
            .setName("table_foo-name")
            .setId("table_foo")
            .setDataPath("gs://target_bucket/table_foo")
            // Check that if the existing entity has different schema, the schema gets updated:
            .setSchema(
                new GoogleCloudDataplexV1Schema()
                    .setFields(Arrays.asList(dataplexField("foobar", "STRING", "REQUIRED")))
                    .setUserManaged(false));
    when(dataplexClientMock.listEntities(eq(ZONE_NAME), any()))
        .thenAnswer(
            invocation -> {
              String filter = invocation.getArgument(1);
              if (filter.equals("data_path=\"gs://target_bucket/table_foo\"")) {
                // listEntities Dataplex API method doesn't return schema, only getEntity does.
                return ImmutableList.of(existingEntity.clone().setSchema(null));
              } else {
                return ImmutableList.of();
              }
            });
    when(dataplexClientMock.getEntity("table_foo-name")).thenReturn(existingEntity);

    exceptionRule.expect(IllegalStateException.class);
    exceptionRule.expectMessage("schema is not user-managed");

    DataplexJdbcIngestion.loadDataplexMetadata(
        dataplexClientMock, options, avroSchema, "gs://target_bucket/table_foo", false);
  }

  private static GoogleCloudDataplexV1Entity defaultDataplexEntity() {
    return new GoogleCloudDataplexV1Entity()
        .setAsset(DataplexUtils.getShortAssetNameFromAsset(ASSET_NAME))
        .setType(EntityType.TABLE.name())
        .setSystem(StorageSystem.CLOUD_STORAGE.name())
        .setSchema(new GoogleCloudDataplexV1Schema().setUserManaged(true))
        .setFormat(
            new GoogleCloudDataplexV1StorageFormat()
                .setMimeType(StorageFormat.PARQUET.getMimeType())
                // Default compression for this template is Snappy, but it's not yet supported
                // by Dataplex, so it has to be mapped to UNSPECIFIED.
                .setCompressionFormat(CompressionFormat.COMPRESSION_FORMAT_UNSPECIFIED.name()));
  }

  private static GoogleCloudDataplexV1SchemaSchemaField dataplexField(
      String name, String type, String mode) {
    return new GoogleCloudDataplexV1SchemaSchemaField().setName(name).setType(type).setMode(mode);
  }

  private static GoogleCloudDataplexV1SchemaPartitionField dataplexPartitionField(
      String name, String type) {
    return new GoogleCloudDataplexV1SchemaPartitionField().setName(name).setType(type);
  }
}
