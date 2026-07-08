package org.apache.beam.it.gcp.datastream.conditions;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.it.gcp.storage.GcsResourceManager;

public class DlqEventsCounter {

  public static long calculateTotalEvents(GcsResourceManager resourceManager, String gcsPathPrefix)
      throws IOException {
    List<Artifact> artifacts = resourceManager.listArtifacts(gcsPathPrefix, Pattern.compile(".*"));
    long totalEvents = 0;
    for (Artifact artifact : artifacts) {
      GcsArtifact gcsArtifact = (GcsArtifact) artifact;
      // Skip the directory placeholder objects
      if (!gcsArtifact.getBlob().getName().endsWith("/")) {
        try (BufferedReader reader =
            new BufferedReader(
                Channels.newReader(gcsArtifact.getBlob().reader(), StandardCharsets.UTF_8))) {
          totalEvents += reader.lines().count();
        }
      }
    }
    return totalEvents;
  }
}
