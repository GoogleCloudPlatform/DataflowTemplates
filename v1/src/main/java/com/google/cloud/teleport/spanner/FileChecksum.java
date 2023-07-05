/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.spanner;

import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;

/** Find the MD5 checksum for files either on local file system or on GCS. */
public class FileChecksum {

  /**
   * Calculate the checksum of a local file.
   *
   * @param p - Path of the file.
   * @return Base64 encoded string representing the MD5 hash of the file.
   */
  public static String getLocalFileChecksum(Path p) {
    try {
      return Base64.getEncoder()
          .encodeToString(Files.asByteSource(p.toFile()).hash(Hashing.md5()).asBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Calculate the checksums of a set of files on GCS.
   *
   * @param gcsUtil - Used to retrieve the files.
   * @param gcsPaths - List of paths of the files.
   * @return A List of Strings representing the MD5 hashes of the files.
   */
  public static List<String> getGcsFileChecksums(GcsUtil gcsUtil, List<GcsPath> gcsPaths) {
    List<String> checksums = new ArrayList<>();
    try {
      for (StorageObjectOrIOException objectOrIOException : gcsUtil.getObjects(gcsPaths)) {
        IOException ex = objectOrIOException.ioException();
        if (ex != null) {
          throw ex;
        }
        checksums.add(objectOrIOException.storageObject().getMd5Hash());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return checksums;
  }
}
