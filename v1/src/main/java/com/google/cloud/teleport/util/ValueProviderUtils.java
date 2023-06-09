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
package com.google.cloud.teleport.util;

import com.google.cloud.teleport.util.DualInputNestedValueProvider.TranslatorInput;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * The {@link ValueProviderUtils} class provides common utilities for ValueProvider, such as
 * assembling DefaultDeadletterTable etc.
 */
public class ValueProviderUtils {

  /**
   * Gets a {@link ValueProvider} for the DefaultDeadletterTable name. If deadletterTable is
   * available, it is returned as is, otherwise outputTableSpec + defaultDeadLetterTableSuffix is
   * returned instead.
   *
   * @param deadletterTable The user provided deadLetterTable.
   * @param outputTableSpec The output table spec.
   * @param defaultDeadLetterTableSuffix The default suffix used to generate the table name.
   * @return The {@link ValueProvider} for the DefaultDeadletterTable name.
   */
  public static ValueProvider<String> maybeUseDefaultDeadletterTable(
      ValueProvider<String> deadletterTable,
      ValueProvider<String> outputTableSpec,
      String defaultDeadLetterTableSuffix) {
    return DualInputNestedValueProvider.of(
        deadletterTable,
        outputTableSpec,
        new SerializableFunction<TranslatorInput<String, String>, String>() {
          @Override
          public String apply(TranslatorInput<String, String> input) {
            String userProvidedTable = input.getX();
            String outputTableSpec = input.getY();
            if (userProvidedTable == null) {
              return outputTableSpec + defaultDeadLetterTableSuffix;
            }
            return userProvidedTable;
          }
        });
  }

  /**
   * Gets a {@link ValueProvider} for an optional parameter. If optionalParameter is available, it
   * is returned as is, otherwise defaultParameter is returned instead.
   *
   * @param optionalParameter
   * @param defaultParameter
   * @return {@link ValueProvider}
   */
  public static <T> ValueProvider<T> eitherOrValueProvider(
      ValueProvider<T> optionalParameter, ValueProvider<T> defaultParameter) {
    return DualInputNestedValueProvider.of(
        optionalParameter,
        defaultParameter,
        new SerializableFunction<TranslatorInput<T, T>, T>() {
          @Override
          public T apply(TranslatorInput<T, T> input) {
            return (input.getX() != null) ? input.getX() : input.getY();
          }
        });
  }
}
