package com.google.cloud.teleport.v2.testing.artifacts;

import com.google.cloud.storage.Blob;
import com.google.re2j.Pattern;
import java.io.IOException;
import java.util.List;

public interface ArtifactClient {
  Blob uploadArtifact(String bucket, String gcsPath, String localPath) throws IOException;

  List<Blob> listArtifacts(String bucket, String testDirPath, Pattern regex);

  void deleteTestDir(String bucket, String testDirPath);
}
