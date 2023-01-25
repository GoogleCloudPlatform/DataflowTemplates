/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.spanner;

import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.spanner.MockSpannerServiceImpl;
import com.google.cloud.spanner.MockSpannerServiceImpl.StatementResult;
import com.google.cloud.spanner.Statement;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.spanner.v1.ResultSet;
import com.google.spanner.v1.ResultSetMetadata;
import com.google.spanner.v1.StructType;
import com.google.spanner.v1.StructType.Field;
import com.google.spanner.v1.Type;
import com.google.spanner.v1.TypeCode;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;

/** Provides helper functions for integration tests. */
public class SpannerTestHelper {

  private static final String FAKE_INSTANCE = "fake-instance";
  private static final String FAKE_PROJECT = "fake-project";
  private static final String FAKE_DATABASE = "fake-database";
  private static final String SPANNER_HOST = "my-host";

  private MockSpannerServiceImpl mockSpannerService;
  private MockServiceHelper serviceHelper;

  public void setUp() throws Exception {
    mockSpannerService = new MockSpannerServiceImpl();
    serviceHelper =
        new MockServiceHelper(SPANNER_HOST, Collections.singletonList(mockSpannerService));
    serviceHelper.start();
    serviceHelper.reset();
  }

  public void tearDown() throws NoSuchFieldException, IllegalAccessException {
    serviceHelper.stop();
  }

  protected SpannerConfig getFakeSpannerConfig() {
    RetrySettings quickRetrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelay(org.threeten.bp.Duration.ofMillis(250))
            .setMaxRetryDelay(org.threeten.bp.Duration.ofSeconds(1))
            .setRetryDelayMultiplier(5)
            .setTotalTimeout(org.threeten.bp.Duration.ofSeconds(1))
            .build();
    return SpannerConfig.create()
        .withEmulatorHost(StaticValueProvider.of(SPANNER_HOST))
        .withIsLocalChannelProvider(StaticValueProvider.of(true))
        .withCommitRetrySettings(quickRetrySettings)
        .withExecuteStreamingSqlRetrySettings(quickRetrySettings)
        .withProjectId(FAKE_PROJECT)
        .withInstanceId(FAKE_INSTANCE)
        .withDatabaseId(FAKE_DATABASE);
  }

  protected void mockGetDialect() {
    Statement determineDialectStatement =
        Statement.newBuilder(
                "SELECT 'POSTGRESQL' AS DIALECT\n"
                    + "FROM INFORMATION_SCHEMA.SCHEMATA\n"
                    + "WHERE SCHEMA_NAME='information_schema'\n"
                    + "UNION ALL\n"
                    + "SELECT 'GOOGLE_STANDARD_SQL' AS DIALECT\n"
                    + "FROM INFORMATION_SCHEMA.SCHEMATA\n"
                    + "WHERE SCHEMA_NAME='INFORMATION_SCHEMA' AND CATALOG_NAME=''")
            .build();
    ResultSetMetadata dialectResultSetMetadata =
        ResultSetMetadata.newBuilder()
            .setRowType(
                StructType.newBuilder()
                    .addFields(
                        Field.newBuilder()
                            .setName("dialect")
                            .setType(Type.newBuilder().setCode(TypeCode.STRING).build())
                            .build())
                    .build())
            .build();
    ResultSet dialectResultSet =
        ResultSet.newBuilder()
            .addRows(
                ListValue.newBuilder()
                    .addValues(Value.newBuilder().setStringValue("GOOGLE_STANDARD_SQL").build())
                    .build())
            .setMetadata(dialectResultSetMetadata)
            .build();
    mockSpannerService.putStatementResult(
        StatementResult.query(determineDialectStatement, dialectResultSet));
  }
}
