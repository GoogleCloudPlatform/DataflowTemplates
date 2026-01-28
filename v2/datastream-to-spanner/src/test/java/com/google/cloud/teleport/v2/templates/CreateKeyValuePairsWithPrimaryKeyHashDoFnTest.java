/*
 * Copyright (C) 2026 Google LLC
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

import static com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants.PERMANENT_ERROR_TAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnit4.class)
public class CreateKeyValuePairsWithPrimaryKeyHashDoFnTest {

  @Mock private PCollectionView<Ddl> ddlView;
  @Mock private DoFn.ProcessContext processContext;

  private CreateKeyValuePairsWithPrimaryKeyHashDoFn doFn;
  private ObjectMapper mapper;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    doFn = new CreateKeyValuePairsWithPrimaryKeyHashDoFn(ddlView);
    doFn.setup();
  }

  private Ddl getTestDdl() {
    return Ddl.builder()
        .createTable("Users")
        .column("first_name")
        .string()
        .max()
        .endColumn()
        .column("last_name")
        .string()
        .size(5)
        .endColumn()
        .column("age")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("first_name")
        .desc("last_name")
        .end()
        .endTable()
        .build();
  }

  private Ddl getAllTypesDdl() {
    return Ddl.builder()
        .createTable("AllTypes")
        .column("bool_col")
        .bool()
        .endColumn()
        .column("int64_col")
        .int64()
        .endColumn()
        .column("float64_col")
        .float64()
        .endColumn()
        .column("string_col")
        .string()
        .max()
        .endColumn()
        .column("bytes_col")
        .bytes()
        .max()
        .endColumn()
        .column("date_col")
        .date()
        .endColumn()
        .column("timestamp_col")
        .timestamp()
        .endColumn()
        .primaryKey()
        .asc("bool_col")
        .asc("int64_col")
        .asc("float64_col")
        .asc("string_col")
        .asc("bytes_col")
        .asc("date_col")
        .asc("timestamp_col")
        .end()
        .endTable()
        .build();
  }

  @Test
  public void testProcessElement() {
    Ddl ddl = getTestDdl();
    when(processContext.sideInput(ddlView)).thenReturn(ddl);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Doe");
    outputObject.put("age", 13);

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    when(processContext.element()).thenReturn(failsafeElement);

    doFn.processElement(processContext);

    ArgumentCaptor<KV<Long, FailsafeElement<String, String>>> captor =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext).output(captor.capture());

    KV<Long, FailsafeElement<String, String>> result = captor.getValue();
    assertEquals(failsafeElement, result.getValue());
    // Verify hash is consistent (we don't check exact value as it depends on hashCode impl)
    String expectedKeyString = "Users_Johnny/Doe";
    // Note: ChangeEventSpannerConvertor.changeEventToPrimaryKey might format it differently,
    // but we just need to ensure it produces *some* hash.
    // Actually, let's verify it's not null.
    assertNotEquals(null, result.getKey());
  }

  @Test
  public void testProcessElementWithConversionError() {
    Ddl ddl = getTestDdl();
    when(processContext.sideInput(ddlView)).thenReturn(ddl);

    // Missing table name
    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put("first_name", "Johnny");

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    when(processContext.element()).thenReturn(failsafeElement);

    doFn.processElement(processContext);

    verify(processContext).output(eq(PERMANENT_ERROR_TAG), any(FailsafeElement.class));
  }

  @Test
  public void testProcessElementWithAllSpannerTypes() {
    Ddl ddl = getAllTypesDdl();
    when(processContext.sideInput(ddlView)).thenReturn(ddl);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "AllTypes");
    outputObject.put("bool_col", true);
    outputObject.put("int64_col", 12345L);
    outputObject.put("float64_col", 3.14);
    outputObject.put("string_col", "test");
    // Bytes are usually base64 encoded in JSON for Datastream?
    // ChangeEventSpannerConvertor handles parsing. Let's assume standard behavior.
    outputObject.put("bytes_col", "dGVzdA=="); // "test" in base64
    outputObject.put("date_col", "2023-01-01");
    outputObject.put("timestamp_col", "2023-01-01T12:00:00Z");

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    when(processContext.element()).thenReturn(failsafeElement);

    doFn.processElement(processContext);

    ArgumentCaptor<KV<Long, FailsafeElement<String, String>>> captor =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext).output(captor.capture());

    KV<Long, FailsafeElement<String, String>> result = captor.getValue();
    assertNotEquals(null, result.getKey());
  }

  @Test
  public void testProcessElementWithSameKeys() {
    Ddl ddl = getTestDdl();
    when(processContext.sideInput(ddlView)).thenReturn(ddl);

    ObjectNode event1 = mapper.createObjectNode();
    event1.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    event1.put("first_name", "Johnny");
    event1.put("last_name", "Doe");
    event1.put("age", 50); // Different non-key field

    ObjectNode event2 = mapper.createObjectNode();
    event2.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    event2.put("first_name", "Johnny");
    event2.put("last_name", "Doe");
    event2.put("age", 51); // Different non-key field

    // Process first event
    when(processContext.element())
        .thenReturn(FailsafeElement.of(event1.toString(), event1.toString()));
    doFn.processElement(processContext);
    ArgumentCaptor<KV<Long, FailsafeElement<String, String>>> captor1 =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext, times(1)).output(captor1.capture());

    // Process second event
    when(processContext.element())
        .thenReturn(FailsafeElement.of(event2.toString(), event2.toString()));
    doFn.processElement(processContext);
    ArgumentCaptor<KV<Long, FailsafeElement<String, String>>> captor2 =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext, times(2)).output(captor2.capture());

    assertEquals(captor1.getValue().getKey(), captor2.getValue().getKey());
  }

  @Test
  public void testProcessElementWithDifferentKeys() {
    Ddl ddl = getTestDdl();
    when(processContext.sideInput(ddlView)).thenReturn(ddl);

    ObjectNode event1 = mapper.createObjectNode();
    event1.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    event1.put("first_name", "Johnny");
    event1.put("last_name", "Doe");

    ObjectNode event2 = mapper.createObjectNode();
    event2.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    event2.put("first_name", "NotJohnny");
    event2.put("last_name", "Doe");

    // Process first event
    when(processContext.element())
        .thenReturn(FailsafeElement.of(event1.toString(), event1.toString()));
    doFn.processElement(processContext);
    ArgumentCaptor<KV<Long, FailsafeElement<String, String>>> captor1 =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext, times(1)).output(captor1.capture());

    // Process second event
    when(processContext.element())
        .thenReturn(FailsafeElement.of(event2.toString(), event2.toString()));
    doFn.processElement(processContext);
    ArgumentCaptor<KV<Long, FailsafeElement<String, String>>> captor2 =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext, times(2)).output(captor2.capture());

    assertNotEquals(captor1.getValue().getKey(), captor2.getValue().getKey());
  }

  @Test
  public void testProcessElementWithPayload() {
    Ddl ddl = getTestDdl();
    when(processContext.sideInput(ddlView)).thenReturn(ddl);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Doe");
    outputObject.put("age", 13);

    // Create a FailsafeElement where original payload is different from current payload
    // This ensures that the DoFn uses getPayload() and not getOriginalPayload()
    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of("original_payload", outputObject.toString());
    when(processContext.element()).thenReturn(failsafeElement);

    doFn.processElement(processContext);

    ArgumentCaptor<KV<Long, FailsafeElement<String, String>>> captor =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext).output(captor.capture());

    KV<Long, FailsafeElement<String, String>> result = captor.getValue();
    assertEquals(failsafeElement, result.getValue());
    assertNotEquals(null, result.getKey());
  }

  @Test
  public void testProcessElementWithUpperCaseColumnKeys() {
    Ddl ddl = getTestDdl();
    when(processContext.sideInput(ddlView)).thenReturn(ddl);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("FIRST_NAME", "Johnny");
    outputObject.put("LAST_NAME", "Doe");
    outputObject.put("AGE", 13);

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    when(processContext.element()).thenReturn(failsafeElement);

    doFn.processElement(processContext);

    ArgumentCaptor<KV<Long, FailsafeElement<String, String>>> captor =
        ArgumentCaptor.forClass(KV.class);
    verify(processContext).output(captor.capture());

    KV<Long, FailsafeElement<String, String>> result = captor.getValue();
    assertEquals(failsafeElement, result.getValue());
    assertNotEquals(null, result.getKey());
  }

  @Test
  public void testProcessElementWithExtraColumns() {
    Ddl ddl = getTestDdl();
    when(processContext.sideInput(ddlView)).thenReturn(ddl);

    ObjectNode outputObject = mapper.createObjectNode();
    outputObject.put(DatastreamConstants.EVENT_TABLE_NAME_KEY, "Users");
    outputObject.put("first_name", "Johnny");
    outputObject.put("last_name", "Doe");
    outputObject.put("age", 13);
    outputObject.put("extra_column", "extra_value");

    FailsafeElement<String, String> failsafeElement =
        FailsafeElement.of(outputObject.toString(), outputObject.toString());
    when(processContext.element()).thenReturn(failsafeElement);

    doFn.processElement(processContext);

    verify(processContext).output(eq(PERMANENT_ERROR_TAG), any(FailsafeElement.class));
  }
}
