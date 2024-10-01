/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.storage.conditions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GCSArtifactsCheck}. */
@RunWith(JUnit4.class)
public class GCSArtifactsCheckTest {
  private GcsResourceManager gcsResourceManager = mock(GcsResourceManager.class);

  private GCSArtifactsCheck gcsArtifactsCheck, gcsArtifactsCheckWithContentMatcher;

  Pattern regex = mock(Pattern.class);

  @Before
  public void setUp() {
    // Create the GCSArtifactsCheck instance
    gcsArtifactsCheck =
        GCSArtifactsCheck.builder(gcsResourceManager, "test-folder", regex)
            .setMinSize(1)
            .setMaxSize(5)
            .build();

    gcsArtifactsCheckWithContentMatcher =
        GCSArtifactsCheck.builder(gcsResourceManager, "test-folder", regex)
            .setMinSize(1)
            .setMaxSize(5)
            .setArtifactContentMatcher("\"tableName\":\"Singers")
            .build();
  }

  @Test
  public void testCheckSuccess() {
    Blob blob = mock(Blob.class);
    when(gcsResourceManager.listArtifacts("test-folder", regex))
        .thenReturn(Arrays.asList(new GcsArtifact(blob)));

    // Call the check method
    GCSArtifactsCheck.CheckResult result = gcsArtifactsCheck.check();

    // Verify the result
    assertEquals(true, result.isSuccess());
    assertEquals("Expected between 1 and 5 artifacts and found 1", result.getMessage());

    // Verify that listArtifacts method was called with correct parameters
    verify(gcsResourceManager, times(1)).listArtifacts("test-folder", regex);
  }

  @Test
  public void testCheckSuccessWithContentMatcher() {
    Blob blob = mock(Blob.class);
    Artifact artifact = new GcsArtifact(blob);
    when(gcsResourceManager.listArtifacts("test-folder", regex))
        .thenReturn(Arrays.asList(artifact));
    String artifactContent = "\"tableName\":\"Singers\"";
    when(artifact.contents()).thenReturn(artifactContent.getBytes());

    GCSArtifactsCheck.CheckResult result = gcsArtifactsCheckWithContentMatcher.check();

    assertEquals(true, result.isSuccess());
    assertEquals("Expected between 1 and 5 artifacts and found 1", result.getMessage());

    verify(gcsResourceManager, times(1)).listArtifacts("test-folder", regex);
  }

  @Test
  public void testCheckFailureWithContentMatcher() {
    Blob blob = mock(Blob.class);
    Artifact artifact = new GcsArtifact(blob);
    when(gcsResourceManager.listArtifacts("test-folder", regex))
        .thenReturn(Arrays.asList(artifact));
    String artifactContent = "\"tableName\":\"Albums\"";
    when(artifact.contents()).thenReturn(artifactContent.getBytes());

    GCSArtifactsCheck.CheckResult result = gcsArtifactsCheckWithContentMatcher.check();

    assertEquals(false, result.isSuccess());
    assertEquals(
        "Expected 1 artifacts with content matcher \"tableName\":\"Singers but has only 0",
        result.getMessage());

    verify(gcsResourceManager, times(1)).listArtifacts("test-folder", regex);
  }

  @Test
  public void testCheck_Failure() {
    // Mock the listArtifacts method to return an empty list
    when(gcsResourceManager.listArtifacts("test-folder", regex)).thenReturn(Arrays.asList());

    // Call the check method
    GCSArtifactsCheck.CheckResult result = gcsArtifactsCheck.check();

    // Verify the result
    assertEquals(false, result.isSuccess());
    assertEquals("Expected 1 artifacts but has only 0", result.getMessage());

    // Verify that listArtifacts method was called with correct parameters
    verify(gcsResourceManager, times(1)).listArtifacts("test-folder", regex);
  }
}
