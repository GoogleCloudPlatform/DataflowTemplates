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
package org.apache.beam.sdk.io.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.apache.beam.sdk.schemas.Schema;

/**
 * This class is used to expose the package level method {@link SchemaUtil#toBeamSchema} to the
 * Teleport project.
 *
 * This util class is temporary until {@link SchemaUtil#toBeamSchema} is made public in Apache Beam.
 */
public final class BeamSchemaUtil {

  private BeamSchemaUtil() {}

  public static Schema toBeamSchema(ResultSetMetaData md) throws SQLException {
    return SchemaUtil.toBeamSchema(md);
  }

  public static SchemaUtil.BeamRowMapper of(Schema schema) {
    return SchemaUtil.BeamRowMapper.of(schema);
  }
}
