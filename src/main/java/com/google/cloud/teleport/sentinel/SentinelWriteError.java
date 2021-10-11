package com.google.cloud.teleport.sentinel;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/**
 * A class for capturing errors writing {@link SentinelEvent}s to Sentinel's Http end point.
 */
@AutoValue
public abstract class SentinelWriteError {
    
    public static Builder newBuilder() {
        return new AutoValue_SentinelWriteError.Builder();
    }

    @Nullable
    public abstract Integer statusCode();
  
    @Nullable
    public abstract String statusMessage();
  
    @Nullable
    public abstract String payload();
  
    @AutoValue.Builder
    abstract static class Builder {
  
      abstract Builder setStatusCode(Integer statusCode);
  
      abstract Integer statusCode();
  
      abstract Builder setStatusMessage(String statusMessage);
  
      abstract Builder setPayload(String payload);
  
      abstract SentinelWriteError autoBuild();
  
      public Builder withStatusCode(Integer statusCode) {
        checkNotNull(statusCode, "withStatusCode(statusCode) called with null input.");
  
        return setStatusCode(statusCode);
      }
  
      public Builder withStatusMessage(String statusMessage) {
        checkNotNull(statusMessage, "withStatusMessage(statusMessage) called with null input.");
  
        return setStatusMessage(statusMessage);
      }
  
      public Builder withPayload(String payload) {
        checkNotNull(payload, "withPayload(payload) called with null input.");
  
        return setPayload(payload);
      }
  
      public SentinelWriteError build() {
        return autoBuild();
      }
    }
  
}
