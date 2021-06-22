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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.FailsafeElementTransforms.ConvertFailsafeElementToPubsubMessage;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link FailsafeElementTransforms}. */
@RunWith(JUnit4.class)
public final class FailsafeElementTransformsTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final Coder<FailsafeElement<String, String>> STR_STR_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());
  private static final SerializableFunction<String, Byte[]> STR_SERIALIZE =
      str -> ArrayUtils.toObject(str.getBytes(StandardCharsets.UTF_8));

  @Test
  public void testFailsafeElementToPubsubMessageOriginalCoder() {
    String original = "Original";
    String current = "Current";

    PCollection<String> actual =
        pipeline
            .apply(
                "Create", Create.of(FailsafeElement.of(original, current)).withCoder(STR_STR_CODER))
            .apply(
                "Convert",
                ConvertFailsafeElementToPubsubMessage.<String, String>builder()
                    .setOriginalPayloadSerializeFn(STR_SERIALIZE)
                    .build())
            .apply(
                "Map",
                MapElements.into(TypeDescriptors.strings())
                    .via(message -> new String(message.getPayload(), StandardCharsets.UTF_8)));

    PAssert.that(actual).containsInAnyOrder(original);

    pipeline.run();
  }

  @Test
  public void testFailsafeElementToPubsubMessageCurrentCoder() {
    String original = "Original";
    String current = "Current";

    PCollection<String> actual =
        pipeline
            .apply(
                "Create", Create.of(FailsafeElement.of(original, current)).withCoder(STR_STR_CODER))
            .apply(
                "Convert",
                ConvertFailsafeElementToPubsubMessage.<String, String>builder()
                    .setCurrentPayloadSerializeFn(STR_SERIALIZE)
                    .build())
            .apply(
                "Map",
                MapElements.into(TypeDescriptors.strings())
                    .via(message -> new String(message.getPayload(), StandardCharsets.UTF_8)));

    PAssert.that(actual).containsInAnyOrder(current);

    pipeline.run();
  }

  @Test(expected = IllegalStateException.class)
  public void testFailsafeElementToPubsubMessageNoCoder() {
    ConvertFailsafeElementToPubsubMessage.<String, String>builder().build();
  }

  @Test(expected = IllegalStateException.class)
  public void testFailsafeElementToPubsubMessageTwoCoders() {
    ConvertFailsafeElementToPubsubMessage.<String, String>builder()
        .setOriginalPayloadSerializeFn(STR_SERIALIZE)
        .setCurrentPayloadSerializeFn(STR_SERIALIZE)
        .build();
  }

  @Test
  public void testFailsafeElementToPubsubMessageErrorWithoutAttribute() {
    String attributeKey = "attrKey";
    String notFoundValue = "notFound";

    PCollection<String> actual =
        pipeline
            .apply(
                "Create",
                Create.of(FailsafeElement.of("original", "current").setErrorMessage("Some error"))
                    .withCoder(STR_STR_CODER))
            .apply(
                "Convert",
                ConvertFailsafeElementToPubsubMessage.<String, String>builder()
                    .setCurrentPayloadSerializeFn(STR_SERIALIZE)
                    .build())
            .apply(
                "Map",
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        message -> {
                          try {
                            return checkNotNull(message.getAttribute(attributeKey));
                          } catch (NullPointerException e) {
                            return notFoundValue;
                          }
                        }));

    PAssert.that(actual).containsInAnyOrder(notFoundValue);

    pipeline.run();
  }

  @Test
  public void testFailsafeElementToPubsubErrorKeyWithoutMessage() {
    String attributeKey = "attrKey";
    String notFoundValue = "notFound";

    PCollection<String> actual =
        pipeline
            .apply(
                "Create",
                Create.of(FailsafeElement.of("original", "current")).withCoder(STR_STR_CODER))
            .apply(
                "Convert",
                ConvertFailsafeElementToPubsubMessage.<String, String>builder()
                    .setCurrentPayloadSerializeFn(STR_SERIALIZE)
                    .setErrorMessageAttributeKey(attributeKey)
                    .build())
            .apply(
                "Map",
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        message -> {
                          try {
                            return checkNotNull(message.getAttribute(attributeKey));
                          } catch (NullPointerException e) {
                            return notFoundValue;
                          }
                        }));

    PAssert.that(actual).containsInAnyOrder(notFoundValue);

    pipeline.run();
  }

  @Test
  public void testFailsafeElementToPubsubErrorKeyAndMessage() {
    String attributeKey = "attrKey";
    String errorMessage = "Some error";
    String notFoundValue = "notFound";

    PCollection<String> actual =
        pipeline
            .apply(
                "Create",
                Create.of(FailsafeElement.of("original", "current").setErrorMessage(errorMessage))
                    .withCoder(STR_STR_CODER))
            .apply(
                "Convert",
                ConvertFailsafeElementToPubsubMessage.<String, String>builder()
                    .setCurrentPayloadSerializeFn(STR_SERIALIZE)
                    .setErrorMessageAttributeKey(attributeKey)
                    .build())
            .apply(
                "Map",
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        message -> {
                          try {
                            return checkNotNull(message.getAttribute(attributeKey));
                          } catch (NullPointerException e) {
                            return notFoundValue;
                          }
                        }));

    PAssert.that(actual).containsInAnyOrder(errorMessage);

    pipeline.run();
  }
}
