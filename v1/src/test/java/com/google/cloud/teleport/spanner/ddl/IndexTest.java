/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.spanner.ddl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.text.IsEqualCompressingWhiteSpace.equalToCompressingWhiteSpace;

import com.google.cloud.spanner.Dialect;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

/** Unit tests for {@link Index} class. */
public class IndexTest {

  @Test
  public void pgIndexPrintsClausesInGrammarOrder() {
    Index index =
        Index.builder(Dialect.POSTGRESQL)
            .name("idx")
            .table("child")
            .unique()
            .interleaveIn("parent")
            .filter("\"val\" IS NOT NULL")
            .options(ImmutableList.of("locality_group='lg'"))
            .columns()
            .create()
            .name("id")
            .asc()
            .endIndexColumn()
            .create()
            .name("val")
            .asc()
            .endIndexColumn()
            .create()
            .name("other")
            .storing()
            .endIndexColumn()
            .end()
            .build();

    assertThat(
        index.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE UNIQUE INDEX \"idx\" ON \"child\"(\"id\" ASC, \"val\" ASC) INCLUDE (\"other\")"
                + " LOCALITY GROUP \"lg\" INTERLEAVE IN \"parent\" WHERE \"val\" IS NOT NULL"));
  }

  @Test
  public void pgVectorIndexPrintsOptionsBeforeLocalityGroup() {
    Index index =
        Index.builder(Dialect.POSTGRESQL)
            .name("vi")
            .table("t")
            .type("ScaNN")
            .filter("\"emb\" IS NOT NULL")
            .options(ImmutableList.of("distance_type='COSINE'", "locality_group='lg'"))
            .columns()
            .create()
            .name("emb")
            .none()
            .endIndexColumn()
            .end()
            .build();

    assertThat(
        index.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE INDEX \"vi\" ON \"t\" USING ScaNN (\"emb\" ) WITH (distance_type='COSINE')"
                + " LOCALITY GROUP \"lg\" WHERE \"emb\" IS NOT NULL"));
  }

  @Test
  public void pgIndexNormalizesLocalityGroupName() {
    assertThat(
        pgIndexWithOption("locality_group=\"lg\""),
        equalToCompressingWhiteSpace(pgIndexPrefix() + " LOCALITY GROUP \"lg\""));
    assertThat(
        pgIndexWithOption("locality_group=lg"),
        equalToCompressingWhiteSpace(pgIndexPrefix() + " LOCALITY GROUP \"lg\""));
    assertThat(
        pgIndexWithOption(" LOCALITY_GROUP = 'lg' "),
        equalToCompressingWhiteSpace(pgIndexPrefix() + " LOCALITY GROUP \"lg\""));
    assertThat(
        pgIndexWithOption("locality_group=x"),
        equalToCompressingWhiteSpace(pgIndexPrefix() + " LOCALITY GROUP \"x\""));
    assertThat(
        pgIndexWithOption("locality_group=NULL"),
        equalToCompressingWhiteSpace(pgIndexPrefix() + " LOCALITY GROUP NULL"));
    // Only a matching pair of quotes is stripped.
    assertThat(
        pgIndexWithOption("locality_group='lg"),
        equalToCompressingWhiteSpace(pgIndexPrefix() + " LOCALITY GROUP \"'lg\""));
  }

  @Test
  public void pgSearchIndexPrintsInterleaveBeforeOptions() {
    Index index =
        Index.builder(Dialect.POSTGRESQL)
            .name("si")
            .table("t")
            .type("SEARCH")
            .interleaveIn("parent")
            .partitionBy(ImmutableList.of("p"))
            .orderBy(ImmutableList.of("o"))
            .options(ImmutableList.of("sort_order_sharding=TRUE"))
            .columns()
            .create()
            .name("tok")
            .none()
            .endIndexColumn()
            .create()
            .name("d")
            .storing()
            .endIndexColumn()
            .end()
            .build();

    assertThat(
        index.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE SEARCH INDEX \"si\" ON \"t\"(\"tok\" ) INCLUDE (\"d\") PARTITION BY \"p\""
                + " ORDER BY \"o\" INTERLEAVE IN \"parent\" WITH (sort_order_sharding=TRUE)"));
  }

  @Test
  public void pgSearchIndexNeverPrintsLocalityGroup() {
    // CREATE SEARCH INDEX has no LOCALITY GROUP clause in PostgreSQL.
    Index index =
        Index.builder(Dialect.POSTGRESQL)
            .name("si")
            .table("t")
            .type("SEARCH")
            .options(ImmutableList.of("locality_group='lg'"))
            .columns()
            .create()
            .name("tok")
            .none()
            .endIndexColumn()
            .end()
            .build();

    assertThat(
        index.prettyPrint(),
        equalToCompressingWhiteSpace("CREATE SEARCH INDEX \"si\" ON \"t\"(\"tok\" )"));
  }

  @Test
  public void gsqlIndexPrintsLocalityGroupInOptionsAfterInterleave() {
    Index index =
        Index.builder(Dialect.GOOGLE_STANDARD_SQL)
            .name("idx")
            .table("child")
            .unique()
            .interleaveIn("parent")
            .filter("val IS NOT NULL")
            .options(ImmutableList.of("locality_group=\"lg\""))
            .columns()
            .create()
            .name("id")
            .asc()
            .endIndexColumn()
            .create()
            .name("val")
            .asc()
            .endIndexColumn()
            .create()
            .name("other")
            .storing()
            .endIndexColumn()
            .end()
            .build();

    assertThat(
        index.prettyPrint(),
        equalToCompressingWhiteSpace(
            "CREATE UNIQUE INDEX `idx` ON `child`(`id` ASC, `val` ASC) STORING (`other`)"
                + " WHERE val IS NOT NULL, INTERLEAVE IN `parent`"
                + " OPTIONS (locality_group=\"lg\")"));
  }

  private static String pgIndexPrefix() {
    return "CREATE INDEX \"idx\" ON \"t\"(\"c\" ASC)";
  }

  private static String pgIndexWithOption(String option) {
    return Index.builder(Dialect.POSTGRESQL)
        .name("idx")
        .table("t")
        .options(ImmutableList.of(option))
        .columns()
        .create()
        .name("c")
        .asc()
        .endIndexColumn()
        .end()
        .build()
        .prettyPrint();
  }
}
