/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.auth.dbauth;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.options.ValueProvider;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.identityconnectors.common.security.GuardedString;

/**
 * Utility Class to wrap the password in a {@link GuardedString}. Wrapping the password in {@link
 * GuardedString} helps prevent accidental logging of the password from the reader code. {@link
 * GuardedString} also zeros the string before it is freed.
 */
public final class GuardedStringValueProvider implements ValueProvider<String>, Serializable {
  private GuardedString guardedString;

  /**
   * Creates a new Instance of {@link GuardedStringValueProvider}.
   *
   * @param value Value to guard
   * @return created instance.
   */
  public static GuardedStringValueProvider create(String value) {
    return new GuardedStringValueProvider(new GuardedString(value.toCharArray()));
  }

  /**
   * Implementation {@link ValueProvider#get()}.
   *
   * @return the wrapped string.
   */
  @Override
  public String get() {
    AtomicReference<String> ret = new AtomicReference<>("");
    this.guardedString().access((clearChars) -> ret.set(new String(clearChars)));
    return ret.get();
  }

  private GuardedString guardedString() {
    return this.guardedString;
  }

  private GuardedStringValueProvider(GuardedString guardedString) {
    this.guardedString = guardedString;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized boolean isAccessible() {
    return true;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    // TODO: wok on an encrypted version of this.
    out.writeObject(this.get());
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    this.guardedString = new GuardedString(((String) in.readObject()).toCharArray());
  }

  private void readObjectNoData() {}
}
