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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.Target;

/**
 * Convenience object for passing Source metadata, Target metadata, PCollection schema, and nullable
 * source rows, together.
 */
public class TargetQuerySpec {

  private final Schema sourceBeamSchema;
  private final PCollection<Row> nullableSourceRows;
  private final Target target;
  private final NodeTarget startNodeTarget;
  private final NodeTarget endNodeTarget;

  private TargetQuerySpec(
      Schema sourceBeamSchema,
      PCollection<Row> nullableSourceRows,
      Target target,
      NodeTarget startNodeTarget,
      NodeTarget endNodeTarget) {
    this.sourceBeamSchema = sourceBeamSchema;
    this.nullableSourceRows = nullableSourceRows;
    this.target = target;
    this.startNodeTarget = startNodeTarget;
    this.endNodeTarget = endNodeTarget;
  }

  public Schema getSourceBeamSchema() {
    return sourceBeamSchema;
  }

  public PCollection<Row> getNullableSourceRows() {
    return nullableSourceRows;
  }

  public Target getTarget() {
    return target;
  }

  public NodeTarget getStartNodeTarget() {
    return startNodeTarget;
  }

  public NodeTarget getEndNodeTarget() {
    return endNodeTarget;
  }

  public static class TargetQuerySpecBuilder {

    private Schema sourceBeamSchema;
    private PCollection<Row> nullableSourceRows;
    private Target target;

    private NodeTarget startNodeTarget;
    private NodeTarget endNodeTarget;

    public TargetQuerySpecBuilder sourceBeamSchema(Schema sourceBeamSchema) {
      this.sourceBeamSchema = sourceBeamSchema;
      return this;
    }

    public TargetQuerySpecBuilder nullableSourceRows(PCollection<Row> nullableSourceRows) {
      this.nullableSourceRows = nullableSourceRows;
      return this;
    }

    public TargetQuerySpecBuilder target(Target target) {
      this.target = target;
      return this;
    }

    public TargetQuerySpecBuilder startNodeTarget(NodeTarget target) {
      this.startNodeTarget = target;
      return this;
    }

    public TargetQuerySpecBuilder endNodeTarget(NodeTarget target) {
      this.endNodeTarget = target;
      return this;
    }

    public Target getTarget() {
      return target;
    }

    public TargetQuerySpec build() {
      validate();
      return new TargetQuerySpec(
          sourceBeamSchema, nullableSourceRows, target, startNodeTarget, endNodeTarget);
    }

    private void validate() {
      if (target == null) {
        throw new IllegalStateException("Target must be specified");
      }
      var type = target.getTargetType();
      switch (type) {
        case RELATIONSHIP:
          validateRelationshipTarget((RelationshipTarget) target);
          break;
        case NODE:
        case QUERY:
          if (startNodeTarget != null) {
            throw new IllegalStateException(
                String.format(
                    "Only relationship targets can specify a start node target, this target is of type %s",
                    type));
          }
          if (endNodeTarget != null) {
            throw new IllegalStateException(
                String.format(
                    "Only relationship targets can specify an end node target, this target is of type %s",
                    type));
          }
          break;
        default:
          throw new RuntimeException(String.format("Unsupported target type %s", type));
      }
    }

    private void validateRelationshipTarget(RelationshipTarget relationshipTarget) {
      if (startNodeTarget == null) {
        throw new IllegalStateException(
            "Relationship targets must specify a start node target, none found");
      }
      String expectedStartName = relationshipTarget.getStartNodeReference();
      String actualStartName = startNodeTarget.getName();
      if (!expectedStartName.equals(actualStartName)) {
        throw new IllegalStateException(
            String.format(
                "Specified start node target name \"%s\" does not match expected name \"%s\"",
                actualStartName, expectedStartName));
      }
      if (endNodeTarget == null) {
        throw new IllegalStateException(
            "Relationship targets must specify an end node target, none found");
      }
      String expectedEndName = relationshipTarget.getEndNodeReference();
      String actualEndName = endNodeTarget.getName();
      if (!expectedEndName.equals(actualEndName)) {
        throw new IllegalStateException(
            String.format(
                "Specified end node target name \"%s\" does not match expected name \"%s\"",
                actualEndName, expectedEndName));
      }
    }
  }
}
