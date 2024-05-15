package org.apache.beam.it.gcp.storage.conditions;

import com.google.auto.value.AutoValue;
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

  @Override
  public String getDescription() {
    if (maxSize() != null) {
      return String.format(
          "GCS resource check if folder %s with regex %s has between %d and %d artifacts",
          prefix(), regex(), minSize(), maxSize());
    }
    return String.format(
        "GCS resource check if folder %s with regex %s has %d artifacts",
        prefix(), regex(), minSize());
  }

  @Override
  public CheckResult check() {
    List<Artifact> artifacts = gcsResourceManager().listArtifacts(prefix(), regex());
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

    if (maxSize() != null) {
      return new CheckResult(
          true,
          String.format(
              "Expected between %d and %d artifacts and found %d",
              minSize(), maxSize(), artifacts.size()));
    }

    return new CheckResult(
        true, String.format("Expected at least %d rows and found %d", minSize(), artifacts.size()));
  }

  public static GCSArtifactsCheck.Builder builder(
      GcsResourceManager resourceManager, String prefix) {
    return new AutoValue_GCSArtifactsCheck.Builder()
        .setGcsResourceManager(resourceManager)
        .setPrefix(prefix);
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

    abstract GCSArtifactsCheck autoBuild();

    public GCSArtifactsCheck build() {
      return autoBuild();
    }
  }
}
