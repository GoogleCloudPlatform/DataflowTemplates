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
package com.google.cloud.teleport.v2.common;

import com.google.auto.service.AutoService;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import com.google.cloud.teleport.v2.utils.GCSUtils;
import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.Security;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLServerSocketFactory;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CommonTemplateJvmInitializer performs all the required steps to support CommonTemplateOptions.
 */
@AutoService(JvmInitializer.class)
public class CommonTemplateJvmInitializer implements JvmInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(CommonTemplateJvmInitializer.class);
  private static final Pattern COMMA_PATTERN = Pattern.compile(",");
  private static final Pattern DEFAULT_GCS_PATTERN = Pattern.compile("^gs:\\/\\/");
  private static final Pattern DEFAULT_SECRET_MANAGER_PATTERN =
      Pattern.compile(
          "^projects\\/[^\\n\\r\\/]+\\/secrets\\/[^\\n\\r\\/]+\\/versions\\/[^\\n\\r\\/]+$");
  private Pattern gcsPattern = DEFAULT_GCS_PATTERN;
  private Pattern secretManagerPattern = DEFAULT_SECRET_MANAGER_PATTERN;
  private String destinationDirectory = "/extra_files";

  @Override
  public void onStartup() {}

  @Override
  public void beforeProcessing(PipelineOptions options) {
    CommonTemplateOptions pipelineOptions = options.as(CommonTemplateOptions.class);
    // disabledAlgorithms
    if (pipelineOptions.getDisabledAlgorithms() != null) {
      String value = pipelineOptions.getDisabledAlgorithms();
      // if the user sets disabledAlgorithms to "none" then set "jdk.tls.disabledAlgorithms" to ""
      if (value.equals("none")) {
        value = "";
      }
      LOG.info("disabledAlgorithms is set to {}.", value);
      Security.setProperty("jdk.tls.disabledAlgorithms", value);
      SSLServerSocketFactory fact = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
      LOG.info("Supported Ciper Suites: " + String.join(", ", fact.getSupportedCipherSuites()));
    }

    if (pipelineOptions.getExtraFilesToStage() != null) {
      // FileSystems does not set the default configuration in workers till Pipeline.run
      // Explicitly registering standard file systems.
      FileSystems.setDefaultPipelineOptions(options);
      createDestinationDirectory();
      saveFilesLocally(pipelineOptions.getExtraFilesToStage());
    }
  }

  /**
   * Creates a destination directory with path `/extra_files`, where all files specified in
   * extraFilesToStage will be stored.
   */
  private void createDestinationDirectory() {
    File destRoot = new File(destinationDirectory);
    if (!destRoot.mkdir()) {
      throw new RuntimeException("Could not create destination folder for extraFilesToStage.");
    }
  }

  private void saveFilesLocally(String extraFilesToStage) {
    try {
      for (String source : COMMA_PATTERN.split(extraFilesToStage)) {
        saveFileLocally(source);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void saveFileLocally(String source) throws IOException {
    Matcher gcsMatcher = gcsPattern.matcher(source);
    Matcher secretManagerMatcher = secretManagerPattern.matcher(source);

    if (gcsMatcher.find()) {
      saveGcsFile(source);
    } else if (secretManagerMatcher.find()) {
      saveSecretPayloadToFile(source);
    } else {
      throw new RuntimeException(
          String.format(
              "Unrecognized source in extraFilesToStage: %s. Please enter a source in the format,"
                  + " gs:// or projects/project-id/secrets/secret-id/versions/version.",
              source));
    }
  }

  private void saveGcsFile(String source) throws IOException {
    byte[] fileData = GCSUtils.getGcsFileAsBytes(source);
    // Filename will be the same as the file in Cloud Storage
    ResourceId sourceResourceId = FileSystems.matchNewResource(source, /*isDirectory*/ false);
    File destFile = Paths.get(destinationDirectory, sourceResourceId.getFilename()).toFile();
    copy(fileData, destFile);
    LOG.info("Localized {} to {}.", source, destFile.getAbsolutePath());
  }

  private void saveSecretPayloadToFile(String source) throws IOException {
    SecretVersionName secretVersionName = parseSecretVersion(source);
    byte[] fileData = getSecretPayload(secretVersionName);
    // Filename will be the secret id
    File destFile = Paths.get(destinationDirectory, secretVersionName.getSecret()).toFile();
    copy(fileData, destFile);
    LOG.info("Localized {} to {}.", source, destFile.getAbsolutePath());
  }

  /**
   * Parses a Secret Version and returns a {@link SecretVersionName}.
   *
   * @param secret Secret Version of the form
   *     projects/{project}/secrets/{secret}/versions/{secret_version}
   * @return {@link SecretVersionName}
   */
  private SecretVersionName parseSecretVersion(String secret) {
    if (SecretVersionName.isParsableFrom(secret)) {
      return SecretVersionName.parse(secret);
    } else {
      throw new IllegalArgumentException(
          "Provided Secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
  }

  /** Extract secret payload from a secret manager secret. */
  private byte[] getSecretPayload(SecretVersionName secretVersionName) throws IOException {
    SecretManagerServiceClient client = SecretManagerServiceClient.create();
    AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
    return response.getPayload().getData().toByteArray();
  }

  /** Copies the byte array into the file. */
  private void copy(byte[] data, File destFile) throws IOException {
    FileOutputStream outputStream = new FileOutputStream(destFile);
    outputStream.write(data);
    outputStream.close();
  }

  /** Only to be used by tests. */
  @VisibleForTesting
  void withDestinationDirectory(String destinationDirectory) {
    this.destinationDirectory = destinationDirectory;
  }

  @VisibleForTesting
  void withFileSystemPattern(String fileSystemPattern) {
    this.gcsPattern = Pattern.compile(fileSystemPattern);
  }

  @VisibleForTesting
  void withSecretManagerPattern(String secretManagerPattern) {
    this.secretManagerPattern = Pattern.compile(secretManagerPattern);
  }
}
