/*
 * Copyright (C) 2023 Google LLC
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
import com.google.cloud.teleport.plugin.model.ImageSpecParameter;
import com.google.cloud.teleport.plugin.model.ImageSpecParameterType;

/**
 * Models a terraform variable definition. See <a
 * href="https://developer.hashicorp.com/terraform/language/values/variables">Input Variables</a>
 * for more information.
 */
@AutoValue
abstract class TerraformVariable {

  /** Name that identifies the variable. */
  abstract String getName();

  /** Specifies what value types are accepted for the variable. */
  abstract Type getType();

  /** This specifies the input variable's documentation. */
  abstract String getDescription();

  /** Specify if the variable can be null within the module. */
  abstract Boolean getNullable();

  static Builder builder() {
    return new AutoValue_TerraformVariable.Builder();
  }

  static TerraformVariable from(ImageSpecParameter parameter) {
    return TerraformVariable.builder()
        .setName(parameter.getName())
        .setDescription(parameter.getHelpText())
        .setType(type(parameter.getParamType()))
        .setNullable(parameter.isOptional())
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setName(String value);

    abstract Builder setDescription(String value);

    abstract Builder setType(Type value);

    abstract Builder setNullable(Boolean value);

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

    /**
     * A (or tuple): a sequence of values, like ["us-west-1a", "us-west-1c"]. Identify elements in a
     * list with consecutive whole numbers, starting with zero.
     */
    LIST,

    /** A collection of unique values that do not have any secondary identifiers or ordering. */
    SET,

    /**
     * A (or object): a group of values identified by named labels, like {name = "Mabel", age = 52}.
     */
    MAP,
  }

  private static TerraformVariable.Type type(ImageSpecParameterType specParameterType) {
    switch (specParameterType) {
      case NUMBER:
        return Type.NUMBER;
      case BOOLEAN:
        return Type.BOOL;
      default:
        return Type.STRING;
    }
  }
}
