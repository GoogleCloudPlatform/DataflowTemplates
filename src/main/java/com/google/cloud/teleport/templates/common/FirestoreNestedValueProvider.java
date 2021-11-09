/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.templates.common;

import com.google.cloud.teleport.util.DualInputNestedValueProvider;
import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * Creates a DualInputNestedValue Provider from two Value Providers. Used to select input from
 * firestore parameters over datastore parameters.
 */
public class FirestoreNestedValueProvider
    extends DualInputNestedValueProvider<String, String, String> {

  private static class FirestoreTranslatorInput
      implements SerializableFunction<TranslatorInput<String, String>, String> {
    private FirestoreTranslatorInput() {}

    public static FirestoreTranslatorInput of() {
      return new FirestoreTranslatorInput();
    }

    @Override
    public String apply(TranslatorInput<String, String> input) {
      String datastoreInput = input.getX();
      String firestoreInput = input.getY();
      if (firestoreInput != null) {
        return firestoreInput;
      }
      return datastoreInput;
    }
  }

  public FirestoreNestedValueProvider(
      ValueProvider<String> datastoreInput, ValueProvider<String> firestoreInput) {
    super(datastoreInput, firestoreInput, FirestoreTranslatorInput.of());
  }
}
