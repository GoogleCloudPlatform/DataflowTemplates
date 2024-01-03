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
package com.google.cloud.teleport.plugin.terraform;

import com.google.auto.value.AutoValue;
import java.util.List;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Models a terraform variable definition. See <a
 * href="https://developer.hashicorp.com/terraform/language/values/variables">Input Variables</a>
 * for more information.
 */
@AutoValue
public abstract class TerraformVariable {

  /** Name that identifies the variable. */
  public abstract String getName();

  /** Specifies what value types are accepted for the variable. */
  public abstract Type getType();

  /** This specifies the input variable's documentation. */
  public abstract String getDescription();

  /** Configures validation of the variable input. */
  @Nullable
  public abstract List<String> getRegexes();

  /** Sets the default value if none provided. */
  @Nullable
  public abstract String getDefaultValue();

  static Builder builder() {
    return new AutoValue_TerraformVariable.Builder();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setName(String value);

    abstract Builder setDescription(String value);

    abstract Builder setType(Type value);

    abstract Builder setDefaultValue(String value);

    abstract Builder setRegexes(List<String> value);

    abstract TerraformVariable build();
  }

  /**
   * Models the variable type. See <a
   * href="https://developer.hashicorp.com/terraform/language/expressions/types#types">...</a> for
   * available types.
   */
  enum Type {
    /** A sequence of Unicode characters representing some text, like "hello". */
    STRING,

    /**
     * A numeric value. The number type can represent both whole numbers like 15 and fractional
     * values like 6.283185.
     */
    NUMBER,

    /** A boolean value, either true or false. bool values can be used in conditional logic. */
    BOOL,
  }
}
