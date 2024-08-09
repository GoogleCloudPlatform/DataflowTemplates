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
package com.google.cloud.teleport.v2.neo4j.providers.text;

import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetQuerySpec;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetSequence;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.transforms.CastExpandTargetRowFn;
import com.google.cloud.teleport.v2.neo4j.utils.BeamUtils;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transform that takes a TargetQuerySpec object and products a PCollection. For text providers,
 * nullableSourceBeamRows will never be null. This class applies in-memory SQL transforms on source
 * rows. Beam SQLTransform does not support ORDER BY nicely, therefore ordering must be forbidden.
 */
public class TextTargetToRow extends PTransform<PBegin, PCollection<Row>> {

  private static final Logger LOG = LoggerFactory.getLogger(TextTargetToRow.class);
  private final TargetSequence targetSequence;
  private final TargetQuerySpec targetQuerySpec;
  private final OptionsParams optionsParams;

  public TextTargetToRow(
      OptionsParams optionsParams, TargetSequence targetSequence, TargetQuerySpec targetQuerySpec) {
    this.optionsParams = optionsParams;
    this.targetSequence = targetSequence;
    this.targetQuerySpec = targetQuerySpec;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {

    PCollection<Row> sourceBeamRows = targetQuerySpec.getNullableSourceRows();
    Schema sourceSchema = targetQuerySpec.getSourceBeamSchema();
    Set<String> sourceFieldSet = ModelUtils.getBeamFieldSet(sourceSchema);

    var target = targetQuerySpec.getTarget();
    var startNodeTarget = targetQuerySpec.getStartNodeTarget();
    var endNodeTarget = targetQuerySpec.getEndNodeTarget();
    var targetSchema = BeamUtils.toBeamSchema(target, startNodeTarget, endNodeTarget);
    DoFn<Row, Row> castToTargetRow =
        new CastExpandTargetRowFn(target, startNodeTarget, endNodeTarget, targetSchema);

    // conditionally apply sql to rows.
    if (ModelUtils.targetHasTransforms(target)) {
      String sql =
          getRewritten(
              ModelUtils.getTargetSql(
                  target, startNodeTarget, endNodeTarget, sourceFieldSet, false));
      LOG.info("Target schema: {}", targetSchema);
      LOG.info("Executing SQL on PCOLLECTION: {}", sql);
      PCollection<Row> sqlDataRow =
          sourceBeamRows.apply(
              targetSequence.getSequenceNumber(target) + ": SQLTransform " + target.getName(),
              SqlTransform.query(sql));
      LOG.info("Sql final schema: {}", sqlDataRow.getSchema());
      return sqlDataRow
          .apply(
              targetSequence.getSequenceNumber(target) + ": Cast " + target.getName() + " rows",
              ParDo.of(castToTargetRow))
          .setRowSchema(targetSchema);
    }
    LOG.info("Target schema: {}", targetSchema);
    return sourceBeamRows
        .apply(
            targetSequence.getSequenceNumber(target) + ": Cast " + target.getName() + " rows",
            ParDo.of(castToTargetRow))
        .setRowSchema(targetSchema);
  }

  private String getRewritten(String sql) {
    return ModelUtils.replaceVariableTokens(sql, optionsParams.getTokenMap());
  }
}
