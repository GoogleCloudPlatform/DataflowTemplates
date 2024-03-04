/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;


/**
 * Test class for {@link DataSourceProvider}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DataSourceProviderTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");
  }

  @Test
  public void testDataSourceProvider() {
    SourceDbToSpannerOptions sourceDbToSpannerOptions = PipelineOptionsFactory.as(
        SourceDbToSpannerOptions.class);
    sourceDbToSpannerOptions.setJdbcDriverClassName("org.apache.derby.jdbc.EmbeddedDriver");
    sourceDbToSpannerOptions.setSourceConnectionURL("jdbc:derby:memory:testDB;create=true");
    sourceDbToSpannerOptions.setSourceConnectionProperties("");
    var firstSource = new DataSourceProvider(sourceDbToSpannerOptions).apply(null);
    var secondSource = new DataSourceProvider(sourceDbToSpannerOptions).apply(null);
    // To verify singleton behavior, check that the references are equal.
    assert (firstSource == secondSource);
  }

}
