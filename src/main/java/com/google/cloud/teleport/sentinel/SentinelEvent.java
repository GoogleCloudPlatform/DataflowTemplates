package com.google.cloud.teleport.sentinel;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import javax.annotation.Nullable;

/** A class for Sentinel events. */
@AutoValue
public abstract class SentinelEvent {
    
    public static Builder newBuilder() {
        return new AutoValue_SentinelEvent.Builder();
      }    


      @Nullable
      public abstract Long time();
    
      @Nullable
      public abstract String host();
    
      @Nullable
      public abstract String source();    
      
      @Nullable
      @SerializedName("sourcetype")
      public abstract String sourceType();

      @Nullable
      public abstract String index();
    
      @Nullable
      public abstract JsonObject fields();
    
      @Nullable
      public abstract String event();      

  /** A builder class for creating {@link SplunkEvent} objects. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setTime(Long time);

    abstract Builder setHost(String host);

    abstract Builder setSource(String source);

    abstract Builder setSourceType(String sourceType);

    abstract Builder setIndex(String index);

    abstract Builder setEvent(String event);

    abstract Builder setFields(JsonObject fields);

    abstract String event();

    abstract SentinelEvent autoBuild();

    public Builder withTime(Long time) {
      checkNotNull(time, "withTime(time) called with null input.");

      return setTime(time);
    }

    public Builder withHost(String host) {
      checkNotNull(host, "withHost(host) called with null input.");

      return setHost(host);
    }

    public Builder withSource(String source) {
      checkNotNull(source, "withSource(source) called with null input.");

      return setSource(source);
    }

    public Builder withSourceType(String sourceType) {
      checkNotNull(sourceType, "withSourceType(sourceType) called with null input.");

      return setSourceType(sourceType);
    }

    public Builder withIndex(String index) {
      checkNotNull(index, "withIndex(index) called with null input.");

      return setIndex(index);
    }

    public Builder withFields(JsonObject fields) {
      checkNotNull(fields, "withFields(fields) called with null input.");

      return setFields(fields);
    }

    public Builder withEvent(String event) {
      checkNotNull(event, "withEvent(event) called with null input.");

      return setEvent(event);
    }

    public SentinelEvent build() {
      checkNotNull(event(), "Event information is required.");
      return autoBuild();
    }
  }      
}