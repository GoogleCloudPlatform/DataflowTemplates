/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.metadata;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Annotations that mark a Dataflow Template parameter. */
public final class TemplateParameter {

  /** Template Parameter containing text inputs. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Text {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Regular expressions to validate the parameter. */
    String[] regexes() default "";

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing password inputs. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Password {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing a project ID. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface ProjectId {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing enum options. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Enum {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Enum options, to show the possible values for an option. */
    TemplateEnumOption[] enumOptions();

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  public @interface TemplateEnumOption {
    String value();

    String label() default "";

    String description() default "";
  }

  /** Template Parameter containing integer numerical inputs (32 bits). */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Integer {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing integer numerical inputs (64 bits). */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Long {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing floating point numerical inputs (32 bits). */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Float {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing floating point numerical inputs (64 bits). */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Double {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing logical inputs. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Boolean {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";

    /** Parameter visibility in the UI. */
    boolean hiddenUi() default false;
  }

  /** Template Parameter containing Cloud Storage folder to read. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface GcsReadFolder {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing Cloud Storage folder to write. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface GcsWriteFolder {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing Cloud Storage file to read. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface GcsReadFile {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing Cloud Storage file to write. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface GcsWriteFile {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing a BigQuery table to read/write. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface BigQueryTable {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing a Pub/Sub topic to read/write. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface PubsubTopic {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing a Pub/Sub subscription to read. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface PubsubSubscription {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing a duration of time. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface Duration {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing an encryption key. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface KmsEncryptionKey {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }

  /** Template Parameter containing a date/time input. */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.FIELD, ElementType.METHOD})
  public @interface DateTime {
    /** Order of appearance. */
    int order() default 999;

    /** Name of the parameter. */
    String name() default "";

    /** Group Name of the parameter. */
    String groupName() default "";

    /** If parameter is optional. */
    boolean optional() default false;

    /** Description of the parameter. */
    String description();

    /** Help text of the parameter. */
    String helpText();

    /** Example of the parameter. */
    String example() default "";
  }
}
