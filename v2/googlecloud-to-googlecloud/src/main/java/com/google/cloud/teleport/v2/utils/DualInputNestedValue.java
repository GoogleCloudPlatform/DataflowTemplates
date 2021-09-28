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
package com.google.cloud.teleport.v2.utils;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** */
public class DualInputNestedValue<T, FirstT, SecondT> implements Serializable {

  /** Pair like struct holding two values. */
  public static class TranslatorInput<FirstT, SecondT> {
    private final FirstT x;
    private final SecondT y;

    public TranslatorInput(FirstT x, SecondT y) {
      this.x = x;
      this.y = y;
    }

    public FirstT getX() {
      return x;
    }

    public SecondT getY() {
      return y;
    }
  }

  private final FirstT valueX;
  private final SecondT valueY;
  private final SerializableFunction<TranslatorInput<FirstT, SecondT>, T> translator;
  private transient volatile T cachedValue;

  public DualInputNestedValue(
      FirstT valueX,
      SecondT valueY,
      SerializableFunction<TranslatorInput<FirstT, SecondT>, T> translator) {
    this.valueX = valueX;
    this.valueY = valueY;
    this.translator = translator;
  }

  /** Creates a {@link DualInputNestedValue} that wraps two provided values. */
  public static <T, FirstT, SecondT> DualInputNestedValue<T, FirstT, SecondT> of(
      FirstT valueX,
      SecondT valueY,
      SerializableFunction<TranslatorInput<FirstT, SecondT>, T> translator) {
    DualInputNestedValue<T, FirstT, SecondT> factory =
        new DualInputNestedValue<>(valueX, valueY, translator);
    return factory;
  }

  public T get() {
    if (cachedValue == null) {
      cachedValue = translator.apply(new TranslatorInput<>(valueX, valueY));
    }
    return cachedValue;
  }

  public boolean isAccessible() {
    return (valueX != null) && (valueY != null);
  }

  @Override
  public String toString() {
    if (isAccessible()) {
      return String.valueOf(get());
    }
    return MoreObjects.toStringHelper(this)
        .add("valueX", valueX)
        .add("valueY", valueY)
        .add("translator", translator.getClass().getSimpleName())
        .toString();
  }
}
