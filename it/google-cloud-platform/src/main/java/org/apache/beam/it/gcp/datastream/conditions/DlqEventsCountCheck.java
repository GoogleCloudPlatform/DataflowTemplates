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
package org.apache.beam.it.gcp.datastream.conditions;

import com.google.auto.value.AutoValue;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.gcp.artifacts.GcsArtifact;
import org.apache.beam.it.gcp.storage.GcsResourceManager;

/** ConditionCheck to validate if Spanner has received a certain amount of rows. */
@AutoValue
public abstract class DlqEventsCountCheck extends ConditionCheck {

  abstract GcsResourceManager resourceManager();

  abstract String gcsPathPrefix();

  abstract Integer minEvents();

  @Nullable
  abstract Integer maxEvents();

  @Override
  public String getDescription() {
    if (maxEvents() != null) {
      return String.format(
          "Check if number of events in a given folder %s is between %d and %d",
          gcsPathPrefix(), minEvents(), maxEvents());
    }
    return String.format(
        "Check if number of events in a given folder %s is %d", gcsPathPrefix(), minEvents());
  }

  private long calculateTotalEvents() throws IOException {
    List<Artifact> artifacts =
        resourceManager().listArtifacts(gcsPathPrefix(), Pattern.compile(".*"));
    long totalEvents = 0;
    for (Artifact artifact : artifacts) {
      GcsArtifact gcsArtifact = (GcsArtifact) artifact;
      // Skip the directory placeholder objects
      if (!gcsArtifact.getBlob().getName().endsWith("/") && gcsArtifact.getBlob().getSize() != 0) {
        try (BufferedReader reader =
            new BufferedReader(
                Channels.newReader(gcsArtifact.getBlob().reader(), StandardCharsets.UTF_8))) {
          totalEvents += reader.lines().count();
        }
      }
    }
    return totalEvents;
  }

  @Override
  public CheckResult check() {
    long totalEvents = 0;
    try {
      totalEvents = calculateTotalEvents();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (totalEvents < minEvents()) {
      return new CheckResult(
          false, String.format("Expected %d events but has only %d", minEvents(), totalEvents));
    }
    if (maxEvents() != null && totalEvents > maxEvents()) {
      return new CheckResult(
          false, String.format("Expected up to %d events but found %d", maxEvents(), totalEvents));
    }

    if (maxEvents() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d events and found %d",
              minEvents(), maxEvents(), totalEvents));
    }

    return new CheckResult(
        true, String.format("Expected at least %d events and found %d", minEvents(), totalEvents));
  }

  public static Builder builder(GcsResourceManager resourceManager, String gcsPathPrefix) {
    return new AutoValue_DlqEventsCountCheck.Builder()
        .setResourceManager(resourceManager)
        .setGcsPathPrefix(gcsPathPrefix);
  }

  /** Builder for {@link DlqEventsCountCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setResourceManager(GcsResourceManager resourceManager);

    public abstract Builder setGcsPathPrefix(String gcsPathPrefix);

    public abstract Builder setMinEvents(Integer minEvents);

    public abstract Builder setMaxEvents(Integer maxEvents);

    abstract DlqEventsCountCheck autoBuild();

    public DlqEventsCountCheck build() {
      return autoBuild();
    }
  }
}
