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
package com.google.cloud.teleport.v2.templates.session;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.io.Resources;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

/** Tests for ReadSessionFile class. */
public class ReadSessionFileTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void readSessionFile() throws Exception {
    Path sessionFile = Paths.get(Resources.getResource("session-file.json").getPath());

    Session session = (new ReadSessionFile(sessionFile.toString())).getSession();

    Session expectedSession = getSessionObject();
    // Validates that the session object created is correct.
    assertThat(session, is(expectedSession));
  }

  @Test
  public void readEmptySessionFilePath() throws Exception {
    String sessionFile = null;
    Session session = (new ReadSessionFile(sessionFile)).getSession();
    Session expectedSession = new Session();

    // Validates that the session object created is correct.
    assertThat(session, is(expectedSession));
  }

  @Test(expected = RuntimeException.class)
  public void readSessionFileWithMissingField1() throws Exception {
    Path sessionFile = Files.createTempFile("session-file-without-SpSchema", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(sessionFile, charset)) {
      String jsonString = getSessionStringWithoutSpSchema();
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    Session session = (new ReadSessionFile(sessionFile.toString())).getSession();
  }

  @Test(expected = RuntimeException.class)
  public void readSessionFileWithMissingField2() throws Exception {
    Path sessionFile = Files.createTempFile("session-file-without-SyntheticPKey", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(sessionFile, charset)) {
      String jsonString = getSessionStringWithoutSyntheticPK();
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    Session session = (new ReadSessionFile(sessionFile.toString())).getSession();
  }

  public static Session getSessionObject() {
    // Add Synthetic PKs.
    Map<String, SyntheticPKey> syntheticPKeys = getSyntheticPks();

    // Add SrcSchema.
    Map<String, SrcSchema> srcSchema = getSampleSrcSchema();

    // Add SpSchema.
    Map<String, CreateTable> spSchema = getSampleSpSchema();

    // Add ToSpanner.
    Map<String, NameAndCols> toSpanner = getToSpanner();

    // Add SrcToID.
    Map<String, NameAndCols> srcToId = getSrcToId();

    Session expectedSession = new Session(spSchema, syntheticPKeys, srcSchema);
    expectedSession.setToSpanner(toSpanner);
    expectedSession.setSrcToID(srcToId);
    return expectedSession;
  }

  public static Map<String, SyntheticPKey> getSyntheticPks() {
    Map<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    syntheticPKeys.put("t2", new SyntheticPKey("c6", 0));
    return syntheticPKeys;
  }

  public static Map<String, SrcSchema> getSampleSrcSchema() {
    Map<String, SrcSchema> srcSchema = new HashMap<String, SrcSchema>();
    Map<String, ColumnDef> t1SrcColDefs = new HashMap<String, ColumnDef>();
    t1SrcColDefs.put("c1", new ColumnDef("product_id"));
    t1SrcColDefs.put("c2", new ColumnDef("quantity"));
    t1SrcColDefs.put("c3", new ColumnDef("user_id"));
    srcSchema.put(
        "t1", new SrcSchema("cart", "my_schema", new String[] {"c3", "c1", "c2"}, t1SrcColDefs));

    Map<String, ColumnDef> t2SrcColDefs = new HashMap<String, ColumnDef>();
    t2SrcColDefs.put("c5", new ColumnDef("name"));
    srcSchema.put("t2", new SrcSchema("people", "my_schema", new String[] {"c5"}, t2SrcColDefs));
    return srcSchema;
  }

  public static Map<String, CreateTable> getSampleSpSchema() {
    Map<String, CreateTable> spSchema = new HashMap<String, CreateTable>();
    Map<String, ColumnDef> t1SpColDefs = new HashMap<String, ColumnDef>();
    t1SpColDefs.put("c1", new ColumnDef("new_product_id"));
    t1SpColDefs.put("c2", new ColumnDef("new_quantity"));
    t1SpColDefs.put("c3", new ColumnDef("new_user_id"));
    spSchema.put("t1", new CreateTable("new_cart", new String[] {"c1", "c2", "c3"}, t1SpColDefs));

    Map<String, ColumnDef> t2SpColDefs = new HashMap<String, ColumnDef>();
    t2SpColDefs.put("c5", new ColumnDef("new_name"));
    t2SpColDefs.put("c6", new ColumnDef("synth_id"));
    spSchema.put("t2", new CreateTable("new_people", new String[] {"c5", "c6"}, t2SpColDefs));
    return spSchema;
  }

  public static Map<String, NameAndCols> getToSpanner() {
    Map<String, NameAndCols> toSpanner = new HashMap<String, NameAndCols>();
    Map<String, String> t1Cols = new HashMap<String, String>();
    t1Cols.put("product_id", "new_product_id");
    t1Cols.put("quantity", "new_quantity");
    t1Cols.put("user_id", "new_user_id");
    toSpanner.put("cart", new NameAndCols("new_cart", t1Cols));

    Map<String, String> t2Cols = new HashMap<String, String>();
    t2Cols.put("name", "new_name");
    toSpanner.put("people", new NameAndCols("new_people", t2Cols));
    return toSpanner;
  }

  public static Map<String, NameAndCols> getSrcToId() {
    Map<String, NameAndCols> srcToId = new HashMap<String, NameAndCols>();
    Map<String, String> t1ColIds = new HashMap<String, String>();
    t1ColIds.put("product_id", "c1");
    t1ColIds.put("quantity", "c2");
    t1ColIds.put("user_id", "c3");
    srcToId.put("cart", new NameAndCols("t1", t1ColIds));

    Map<String, String> t2ColIds = new HashMap<String, String>();
    t2ColIds.put("name", "c5");
    srcToId.put("people", new NameAndCols("t2", t2ColIds));
    return srcToId;
  }

  private static String getSessionStringWithoutSpSchema() {
    return "{\n"
        + "    \"SyntheticPKeys\": {\n"
        + "      \"products\": {\n"
        + "        \"Col\": \"synth_id\",\n"
        + "        \"Sequence\": 0\n"
        + "      }\n"
        + "    }\n"
        + "}";
  }

  private static String getSessionStringWithoutSyntheticPK() {
    return "{\n"
        + "    \"ToSpanner\": {\n"
        + "      \"products\": {\n"
        + "        \"Name\": \"products\",\n"
        + "        \"Cols\": {\n"
        + "          \"price\": \"price\",\n"
        + "          \"product_id\": \"product_id\"\n"
        + "        }\n"
        + "      }\n"
        + "    }\n"
        + "}";
  }
}
