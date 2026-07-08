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

import com.google.auto.value.AutoValue;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.storage.GcsResourceManager;

@AutoValue
public abstract class GCSArtifactsCheck extends ConditionCheck {

  abstract GcsResourceManager gcsResourceManager();

  abstract String prefix();

  abstract Pattern regex();

  abstract Integer minSize();

  @Nullable
  abstract Integer maxSize();

  @Nullable
  abstract String artifactContentMatcher();

  @Override
  public String getDescription() {
    if (maxSize() != null) {
      return String.format(
          "GCS resource check if folder path %s with regex %s has between %d and %d artifacts",
          prefix(), regex(), minSize(), maxSize());
    }
    return String.format(
        "GCS resource check if folder %s with regex %s has %d artifacts",
        prefix(), regex(), minSize());
  }

  private int countContentOccurrences(String content, List<Artifact> artifacts) {
    // Initialize the count of occurrences
    int count = 0;
    // Iterate through the artifacts and count occurrences of the content
    for (Artifact artifact : artifacts) {
      String artifactContent = new String(artifact.contents(), StandardCharsets.UTF_8);
      Pattern pattern = Pattern.compile(Pattern.quote(content));
      java.util.regex.Matcher matcher = pattern.matcher(artifactContent);
      while (matcher.find()) {
        count++;
      }
    }
    return count;
  }

  @Override
  public CheckResult check() {
    List<Artifact> artifacts = gcsResourceManager().listArtifacts(prefix(), regex());
    if (artifactContentMatcher() != null) {
      int count = countContentOccurrences(artifactContentMatcher(), artifacts);
      if (count < minSize()) {
        return new CheckResult(
            false,
            String.format(
                "Expected %d artifacts with content matcher %s but has only %d",
                minSize(), artifactContentMatcher(), count));
      }
      if (maxSize() != null && count > maxSize()) {
        return new CheckResult(
            false,
            String.format(
                "Expected up to %d artifacts with content matcher %s but found %d",
                maxSize(), artifactContentMatcher(), count));
      }
    } else {
      if (artifacts.size() < minSize()) {
        return new CheckResult(
            false,
            String.format("Expected %d artifacts but has only %d", minSize(), artifacts.size()));
      }
      if (maxSize() != null && artifacts.size() > maxSize()) {
        return new CheckResult(
            false,
            String.format("Expected up to %d artifacts but found %d", maxSize(), artifacts.size()));
      }
    }
    if (maxSize() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d artifacts and found %d",
              minSize(), maxSize(), artifacts.size()));
    }

    return new CheckResult(
        true,
        String.format("Expected at least %d artifacts and found %d", minSize(), artifacts.size()));
  }

  public static GCSArtifactsCheck.Builder builder(
      GcsResourceManager resourceManager, String prefix, Pattern regex) {
    return new AutoValue_GCSArtifactsCheck.Builder()
        .setGcsResourceManager(resourceManager)
        .setPrefix(prefix)
        .setRegex(regex);
  }

  /** Builder for {@link GCSArtifactsCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract GCSArtifactsCheck.Builder setGcsResourceManager(
        GcsResourceManager gcsResourceManager);

    public abstract GCSArtifactsCheck.Builder setPrefix(String prefix);

    public abstract GCSArtifactsCheck.Builder setRegex(Pattern regex);

    public abstract GCSArtifactsCheck.Builder setMinSize(Integer minSize);

    public abstract GCSArtifactsCheck.Builder setMaxSize(Integer maxSize);

    public abstract GCSArtifactsCheck.Builder setArtifactContentMatcher(
        String artifactContentMatcher);

    abstract GCSArtifactsCheck autoBuild();

    public GCSArtifactsCheck build() {
      return autoBuild();
    }
  }
}
