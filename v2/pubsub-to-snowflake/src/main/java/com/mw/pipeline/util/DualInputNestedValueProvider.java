/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. SecondTou may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANSecondT KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.mw.pipeline.util;

import com.google.common.base.MoreObjects;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.io.Serializable;

/**
 * {@link DualInputNestedValueProvider} is an implementation of {@link ValueProvider} that allows
 * for wrapping two {@link ValueProvider} objects. It's inspired by {@link
 * org.apache.beam.sdk.options.ValueProvider.NestedValueProvider} but it can accept two inputs
 * rather than one.
 */
public class DualInputNestedValueProvider<T, FirstT, SecondT>
        implements ValueProvider<T>, Serializable {

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

    private final ValueProvider<FirstT> valueX;
    private final ValueProvider<SecondT> valueY;
    private final SerializableFunction<TranslatorInput<FirstT, SecondT>, T> translator;
    private transient volatile T cachedValue;

    public DualInputNestedValueProvider(
            ValueProvider<FirstT> valueX,
            ValueProvider<SecondT> valueY,
            SerializableFunction<TranslatorInput<FirstT, SecondT>, T> translator) {
        this.valueX = valueX;
        this.valueY = valueY;
        this.translator = translator;
    }

    /** Creates a {@link NestedValueProvider} that wraps two provided values. */
    public static <T, FirstT, SecondT> DualInputNestedValueProvider<T, FirstT, SecondT> of(
            ValueProvider<FirstT> valueX,
            ValueProvider<SecondT> valueY,
            SerializableFunction<TranslatorInput<FirstT, SecondT>, T> translator) {
        DualInputNestedValueProvider<T, FirstT, SecondT> factory =
                new DualInputNestedValueProvider<>(valueX, valueY, translator);
        return factory;
    }

    @Override
    public T get() {
        if (cachedValue == null) {
            cachedValue = translator.apply(new TranslatorInput<>(valueX.get(), valueY.get()));
        }
        return cachedValue;
    }

    @Override
    public boolean isAccessible() {
        return valueX.isAccessible() && valueY.isAccessible();
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