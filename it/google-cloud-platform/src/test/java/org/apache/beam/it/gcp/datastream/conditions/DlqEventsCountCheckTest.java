package org.apache.beam.it.gcp.datastream.conditions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.it.conditions.ConditionCheck.CheckResult;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

/** Unit tests for {@link DlqEventsCountCheck}. */
@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class DlqEventsCountCheckTest {

  @Rule public MockitoRule mockito = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  @Mock private GcsResourceManager mockResourceManager;

  private static final String GCS_PATH_PREFIX = "gs://bucket/dlq/";
  private static final Storage storage = LocalStorageHelper.getOptions().getService();

  /** Helper method to create a mock GcsArtifact with a REAL in-memory blob. */
  private GcsArtifact createMockArtifact(String name, String content) {
    GcsArtifact artifact = mock(GcsArtifact.class);

    // Create a real blob in the in-memory storage service to get a valid ReadChannel
    BlobId blobId = BlobId.of("bucket", name);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    Blob blob = storage.create(blobInfo, content.getBytes(StandardCharsets.UTF_8));

    when(artifact.getBlob()).thenReturn(blob);
    return artifact;
  }

  @Test
  public void testGetDescriptionWithMinOnly() {
    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX)
            .setMinEvents(10)
            .build();
    assertEquals(
        "Check if number of events in a given folder gs://bucket/dlq/ is 10",
        check.getDescription());
  }

  @Test
  public void testGetDescriptionWithMinAndMax() {
    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX)
            .setMinEvents(10)
            .setMaxEvents(20)
            .build();
    assertEquals(
        "Check if number of events in a given folder gs://bucket/dlq/ is between 10 and 20",
        check.getDescription());
  }

  @Test
  public void testCheckSuccessWhenEventsMet() {
    List<Artifact> mockArtifacts =
        Collections.singletonList(createMockArtifact("file1.txt", "line1\nline2\nline3"));
    when(mockResourceManager.listArtifacts(eq(GCS_PATH_PREFIX), any(Pattern.class))).thenReturn(mockArtifacts);

    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX).setMinEvents(3).build();

    CheckResult result = check.check();

    assertTrue(result.isSuccess());
    assertEquals("Expected at least 3 events and found 3", result.getMessage());
  }

  @Test
  public void testCheckSuccessWhenEventsMetWithMultipleFiles() {
    List<Artifact> mockArtifacts =
        Arrays.asList(
            createMockArtifact("file1.txt", "line1\nline2"),
            createMockArtifact("file2.txt", "line3"));
    when(mockResourceManager.listArtifacts(eq(GCS_PATH_PREFIX), any(Pattern.class))).thenReturn(mockArtifacts);

    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX).setMinEvents(3).build();

    CheckResult result = check.check();

    assertTrue(result.isSuccess());
    assertEquals("Expected at least 3 events and found 3", result.getMessage());
  }

  @Test
  public void testCheckSuccessWhenEventsInRange() {
    List<Artifact> mockArtifacts =
        Collections.singletonList(createMockArtifact("file1.txt", "line1\nline2\nline3\nline4"));
    when(mockResourceManager.listArtifacts(eq(GCS_PATH_PREFIX), any(Pattern.class))).thenReturn(mockArtifacts);

    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX)
            .setMinEvents(3)
            .setMaxEvents(5)
            .build();

    CheckResult result = check.check();

    assertTrue(result.isSuccess());
    assertEquals("Expected between 3 and 5 events and found 4", result.getMessage());
  }

  @Test
  public void testCheckFailureWhenEventsBelowMin() {
    List<Artifact> mockArtifacts =
        Collections.singletonList(createMockArtifact("file1.txt", "line1"));
    when(mockResourceManager.listArtifacts(eq(GCS_PATH_PREFIX), any(Pattern.class))).thenReturn(mockArtifacts);

    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX).setMinEvents(2).build();

    CheckResult result = check.check();

    assertFalse(result.isSuccess());
    assertEquals("Expected 2 events but has only 1", result.getMessage());
  }

  @Test
  public void testCheckFailureWhenEventsAboveMax() {
    List<Artifact> mockArtifacts =
        Collections.singletonList(createMockArtifact("file1.txt", "line1\nline2\nline3"));
    when(mockResourceManager.listArtifacts(eq(GCS_PATH_PREFIX), any(Pattern.class))).thenReturn(mockArtifacts);

    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX)
            .setMinEvents(1)
            .setMaxEvents(2)
            .build();

    CheckResult result = check.check();

    assertFalse(result.isSuccess());
    assertEquals("Expected up to 2 events but found 3", result.getMessage());
  }

  @Test
  public void testCheckSkipsEmptyFilesAndDirectories() {
    List<Artifact> mockArtifacts =
        Arrays.asList(
            createMockArtifact("file1.txt", "line1\nline2"), // 2 lines
            createMockArtifact("empty.txt", ""), // empty file
            createMockArtifact("subdir/", "") // directory placeholder
        );
    when(mockResourceManager.listArtifacts(eq(GCS_PATH_PREFIX), any(Pattern.class))).thenReturn(mockArtifacts);

    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX).setMinEvents(2).build();

    CheckResult result = check.check();

    assertTrue(result.isSuccess());
    assertEquals("Expected at least 2 events and found 2", result.getMessage());
  }

  @Test
  public void testCheckWithNoArtifactsFound() {
    when(mockResourceManager.listArtifacts(eq(GCS_PATH_PREFIX), any(Pattern.class)))
        .thenReturn(Collections.emptyList());

    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX).setMinEvents(1).build();

    CheckResult result = check.check();
    assertFalse(result.isSuccess());
    assertEquals("Expected 1 events but has only 0", result.getMessage());
  }

  @Test(expected = RuntimeException.class)
  public void testCheckThrowsRuntimeExceptionOnIOException() throws IOException {
    GcsArtifact mockArtifact = mock(GcsArtifact.class);
    Blob mockBlob = mock(Blob.class);

    when(mockBlob.reader()).thenThrow(new IOException("Test IO Exception"));

    List<Artifact> mockArtifacts = Collections.singletonList(mockArtifact);
    when(mockResourceManager.listArtifacts(eq(GCS_PATH_PREFIX), any(Pattern.class))).thenReturn(mockArtifacts);

    DlqEventsCountCheck check =
        DlqEventsCountCheck.builder(mockResourceManager, GCS_PATH_PREFIX).setMinEvents(1).build();

    check.check(); // This should throw a RuntimeException wrapping the IOException
  }
}
