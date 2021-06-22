/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.transforms;

import static com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang.ArrayUtils;
import org.apache.parquet.Strings;

/** {@link PTransform}s for working with {@link FailsafeElement}s. */
public final class FailsafeElementTransforms {
  /**
   * {@link PTransform} for converting a {@link FailsafeElement} to {@link PubsubMessage}.
   *
   * <p>The {@link PubsubMessage} payload will be either the original payload or current payload of
   * the {@link FailsafeElement}. Which one depends on the provided {@link SerializableFunction}.
   *
   * <p>There is also support for an optional error message attribute. There is no support for a
   * stack trace attribute, as that can easily exceed Pub/Sub limitations.
   */
  @AutoValue
  public abstract static class ConvertFailsafeElementToPubsubMessage<OriginalT, CurrentT>
      extends PTransform<
          PCollection<FailsafeElement<OriginalT, CurrentT>>, PCollection<PubsubMessage>> {

    /**
     * {@link SerializableFunction} for the result of {@link FailsafeElement#getOriginalPayload()}.
     *
     * <p>Either this or {@link ConvertFailsafeElementToPubsubMessage#currentPayloadSerializeFn()}
     * must return non-null, but not both. The one that doesn't return null will determine which
     * value from the {@link FailsafeElement} to encode.
     */
    @Nullable
    public abstract SerializableFunction<OriginalT, Byte[]> originalPayloadSerializeFn();

    /**
     * {@link SerializableFunction} for the result of {@link FailsafeElement#getPayload()}.
     *
     * <p>Either this or {@link ConvertFailsafeElementToPubsubMessage#originalPayloadSerializeFn()}
     * must return non-null, but not both. The one that doesn't return null will determine which
     * value from the {@link FailsafeElement} to encode.
     */
    @Nullable
    public abstract SerializableFunction<CurrentT, Byte[]> currentPayloadSerializeFn();

    /**
     * Optional key to use for setting a Pub/Sub attribute that contains {@link
     * FailsafeElement#getErrorMessage()} as its value.
     *
     * <p>If this is null or there is error message in the {@link FailsafeElement}, then no
     * attribute will be created.
     */
    @Nullable
    public abstract String errorMessageAttributeKey();

    public static <OriginalT, CurrentT> Builder<OriginalT, CurrentT> builder() {
      return new AutoValue_FailsafeElementTransforms_ConvertFailsafeElementToPubsubMessage
          .Builder<>();
    }

    /** Builder for {@link ConvertFailsafeElementToPubsubMessage}. */
    @AutoValue.Builder
    public abstract static class Builder<OriginalT, CurrentT> {
      public abstract Builder<OriginalT, CurrentT> setOriginalPayloadSerializeFn(
          @Nullable SerializableFunction<OriginalT, Byte[]> value);

      public abstract Builder<OriginalT, CurrentT> setCurrentPayloadSerializeFn(
          @Nullable SerializableFunction<CurrentT, Byte[]> value);

      public abstract Builder<OriginalT, CurrentT> setErrorMessageAttributeKey(
          @Nullable String value);

      protected abstract ConvertFailsafeElementToPubsubMessage<OriginalT, CurrentT> autoBuild();

      public final ConvertFailsafeElementToPubsubMessage<OriginalT, CurrentT> build() {
        ConvertFailsafeElementToPubsubMessage<OriginalT, CurrentT> converter = autoBuild();
        checkState(
            converter.originalPayloadSerializeFn() != null
                ^ converter.currentPayloadSerializeFn() != null,
            "Either originalPayloadCoder or currentPayloadCoder must be set, but not both");
        return converter;
      }
    }

    @Override
    public PCollection<PubsubMessage> expand(
        PCollection<FailsafeElement<OriginalT, CurrentT>> input) {
      return input.apply(
          "FailsafeElement to PubsubMessage DoFn",
          ParDo.of(
              new DoFn<FailsafeElement<OriginalT, CurrentT>, PubsubMessage>() {
                @ProcessElement
                public void processElement(ProcessContext context) {
                  FailsafeElement<OriginalT, CurrentT> element = context.element();

                  byte[] payload = serializePayload(element);

                  ImmutableMap.Builder<String, String> attributes = ImmutableMap.builder();
                  if (!Strings.isNullOrEmpty(errorMessageAttributeKey())
                      && !Strings.isNullOrEmpty(element.getErrorMessage())) {
                    attributes.put(errorMessageAttributeKey(), element.getErrorMessage());
                  }

                  context.output(new PubsubMessage(payload, attributes.build()));
                }
              }));
    }

    /** Handles the encoding regardless of which part of {@code element} needs to be encoded. */
    private byte[] serializePayload(FailsafeElement<OriginalT, CurrentT> element) {
      Byte[] bytes =
          originalPayloadSerializeFn() != null
              ? originalPayloadSerializeFn().apply(element.getOriginalPayload())
              : currentPayloadSerializeFn().apply(element.getPayload());
      return ArrayUtils.toPrimitive(bytes);
    }
  }
}
