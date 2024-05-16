package org.apache.beam.it.gcp.storage.conditions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Blob;
import java.util.Arrays;
import java.util.regex.Pattern;
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

  private GCSArtifactsCheck gcsArtifactsCheck;

  Pattern regex = mock(Pattern.class);

  @Before
  public void setUp() {
    // Create the GCSArtifactsCheck instance
    gcsArtifactsCheck =
        GCSArtifactsCheck.builder(gcsResourceManager, "test-folder", regex)
            .setMinSize(1)
            .setMaxSize(5) // You can set maxSize as needed for testing
            .build();
  }

  @Test
  public void testCheck_Success() {
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
