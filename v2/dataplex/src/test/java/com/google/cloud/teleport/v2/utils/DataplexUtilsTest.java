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
package com.google.cloud.teleport.v2.utils;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaPartitionField;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaSchemaField;
import com.google.cloud.teleport.v2.clients.DataplexClient;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link DataplexUtils}. */
@RunWith(JUnit4.class)
public class DataplexUtilsTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  @Mock private DataplexClient dataplexMock;

  @Test
  public void test_toDataplexSchema_returnsProperDataplexSchema() throws IOException {
    Schema avroSchema =
        new Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":"
                    // Partition key:
                    + "[{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},"
                    // Logical types:
                    + "{\"name\":\"d1\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]},"
                    + "{\"name\":\"t1\",\"type\":{\"type\":\"long\",\"logicalType\":\"time-micros\"}},"
                    + "{\"name\":\"dt\",\"type\":[\"null\",{\"type\":\"string\",\"logicalType\":\"datetime\"}]},"
                    // Array type:
                    + "{\"name\":\"_array_of_str\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
                    // Primitive types:
                    + "{\"name\":\"_string\",\"type\":[\"null\",\"string\"]},"
                    + "{\"name\":\"_float\",\"type\":{\"type\":\"float\"}},"
                    + "{\"name\":\"_double\",\"type\":{\"type\":\"double\"}},"
                    + "{\"name\":\"_boolean\",\"type\":{\"type\":\"boolean\"}},"
                    + "{\"name\":\"_fixed\",\"type\":{\"type\":\"fixed\",\"size\":16,\"name\":\"md5\"}},"
                    + "{\"name\":\"_bytes\",\"type\":{\"type\":\"bytes\"}},"
                    + "{\"name\":\"_int\",\"type\":{\"type\":\"int\"}},"
                    + "{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"AddressRecord\",\"fields\":[{\"name\":\"zip\",\"type\":\"int\"},{\"name\":\"city\",\"type\":[\"null\",\"string\"]}]}},"
                    + "{\"name\":\"_long\",\"type\":[\"null\",\"long\"]}]}");

    GoogleCloudDataplexV1Schema actual = DataplexUtils.toDataplexSchema(avroSchema, "ts");

    GoogleCloudDataplexV1Schema expected =
        new GoogleCloudDataplexV1Schema()
            .setFields(
                Arrays.asList(
                    dataplexField("d1", "DATE", "NULLABLE"),
                    dataplexField("t1", "TIME", "REQUIRED"),
                    // "datetime" is not actually a valid Avro logical type, so parsed as STRING:
                    dataplexField("dt", "STRING", "NULLABLE"),
                    dataplexField("_array_of_str", "STRING", "REPEATED"),
                    dataplexField("_string", "STRING", "NULLABLE"),
                    dataplexField("_float", "FLOAT", "REQUIRED"),
                    dataplexField("_double", "DOUBLE", "REQUIRED"),
                    dataplexField("_boolean", "BOOLEAN", "REQUIRED"),
                    dataplexField("_fixed", "BINARY", "REQUIRED"),
                    dataplexField("_bytes", "BINARY", "REQUIRED"),
                    dataplexField("_int", "INT32", "REQUIRED"),
                    dataplexField(
                        "address",
                        "RECORD",
                        "REQUIRED",
                        dataplexField("zip", "INT32", "REQUIRED"),
                        dataplexField("city", "STRING", "NULLABLE")),
                    dataplexField("_long", "INT64", "NULLABLE")))
            .setPartitionFields(
                Collections.singletonList(dataplexPartitionField("ts", "TIMESTAMP")));

    assertThat(actual).isEqualTo(expected);
  }

  @Test(expected = IllegalArgumentException.class)
  public void test_toDataplexSchema_throwsExceptionIfPartitionKeyNotFoundInSchema() {
    Schema avroSchema =
        new Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"__root__\",\"fields\":"
                    + "[{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}]},"
                    + "{\"name\":\"_long\",\"type\":[\"null\",\"long\"]}]}");

    DataplexUtils.toDataplexSchema(avroSchema, "non_existing_name");
  }

  @Test
  public void test_createEntityWithUniqueId_createsEntityOnFirstTry() throws IOException {
    when(dataplexMock.createEntity(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Entity e = invocation.getArgument(1);
              return e.clone().setName("name_" + e.getId());
            });
    GoogleCloudDataplexV1Entity entity = new GoogleCloudDataplexV1Entity().setId("foo");

    GoogleCloudDataplexV1Entity actual =
        DataplexUtils.createEntityWithUniqueId(dataplexMock, "z1", entity, 10);

    GoogleCloudDataplexV1Entity expected =
        new GoogleCloudDataplexV1Entity().setId("foo").setName("name_foo");
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void test_createEntityWithUniqueId_createsEntityWithNewIdOnFailure() throws IOException {
    GoogleJsonResponseException responseExceptionMock = createEntityException();
    when(dataplexMock.createEntity(any(), any()))
        .thenAnswer(
            invocation -> {
              GoogleCloudDataplexV1Entity e = invocation.getArgument(1);
              if ("foo".equals(e.getId()) || "foo_2".equals(e.getId())) {
                throw responseExceptionMock;
              }
              return e.clone().setName("name_" + e.getId());
            });
    GoogleCloudDataplexV1Entity entity = new GoogleCloudDataplexV1Entity().setId("foo");

    GoogleCloudDataplexV1Entity actual =
        DataplexUtils.createEntityWithUniqueId(dataplexMock, "z1", entity, 10);

    GoogleCloudDataplexV1Entity expected =
        new GoogleCloudDataplexV1Entity().setId("foo_3").setName("name_foo_3");
    assertThat(actual).isEqualTo(expected);
    verify(dataplexMock, times(3)).createEntity(eq("z1"), any());
  }

  @Test
  public void test_createEntityWithUniqueId_failsAfterExceedingMaxAttempts() throws IOException {
    GoogleJsonResponseException responseExceptionMock = createEntityException();
    when(dataplexMock.createEntity(any(), any())).thenThrow(responseExceptionMock);
    GoogleCloudDataplexV1Entity entity = new GoogleCloudDataplexV1Entity().setId("foo");

    try {
      DataplexUtils.createEntityWithUniqueId(dataplexMock, "z1", entity, 4);
    } catch (IOException e) {
      verify(dataplexMock, times(4)).createEntity(any(), any());
      verify(dataplexMock, times(1))
          .createEntity("z1", new GoogleCloudDataplexV1Entity().setId("foo"));
      verify(dataplexMock, times(1))
          .createEntity("z1", new GoogleCloudDataplexV1Entity().setId("foo_2"));
      verify(dataplexMock, times(1))
          .createEntity("z1", new GoogleCloudDataplexV1Entity().setId("foo_3"));
      verify(dataplexMock, times(1))
          .createEntity("z1", new GoogleCloudDataplexV1Entity().setId("foo_4"));
      assertThat(e).hasMessageThat().contains("Exceeded maximum attempts");
      return;
    }

    fail("Expected an IOException.");
  }

  @Test
  public void test_createEntityWithUniqueId_failsFastOnUnexpectedError() throws IOException {
    when(dataplexMock.createEntity(any(), any())).thenThrow(new IOException("Bad request"));
    GoogleCloudDataplexV1Entity entity = new GoogleCloudDataplexV1Entity().setId("foo");

    try {
      DataplexUtils.createEntityWithUniqueId(dataplexMock, "z1", entity, 4);
    } catch (IOException e) {
      verify(dataplexMock, times(1)).createEntity(any(), any());
      verify(dataplexMock, times(1))
          .createEntity("z1", new GoogleCloudDataplexV1Entity().setId("foo"));
      assertThat(e).hasMessageThat().isEqualTo("Bad request");
      return;
    }

    fail("Expected an IOException.");
  }

  private static GoogleCloudDataplexV1SchemaSchemaField dataplexField(
      String name,
      String type,
      String mode,
      GoogleCloudDataplexV1SchemaSchemaField... nestedFields) {
    GoogleCloudDataplexV1SchemaSchemaField f =
        new GoogleCloudDataplexV1SchemaSchemaField().setName(name).setType(type).setMode(mode);
    if (nestedFields.length > 0) {
      f.setFields(Arrays.asList(nestedFields));
    }
    return f;
  }

  private static GoogleCloudDataplexV1SchemaPartitionField dataplexPartitionField(
      String name, String type) {
    return new GoogleCloudDataplexV1SchemaPartitionField().setName(name).setType(type);
  }

  private static GoogleJsonResponseException createEntityException() {
    GoogleJsonResponseException mock =
        mock(GoogleJsonResponseException.class, Answers.RETURNS_DEEP_STUBS);
    when(mock.getStatusCode()).thenReturn(409);
    when(mock.getDetails().getMessage()).thenReturn("entity already exists");
    return mock;
  }
}
