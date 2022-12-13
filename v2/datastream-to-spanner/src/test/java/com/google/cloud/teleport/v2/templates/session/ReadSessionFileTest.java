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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

/** Tests for ReadSessionFile class. */
public class ReadSessionFileTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void readSessionFile() throws Exception {
    Path sessionFile = Files.createTempFile("session-file", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(sessionFile, charset)) {
      String jsonString = getSessionString();
      writer.write(jsonString, 0, jsonString.length());
    } catch (IOException e) {
      e.printStackTrace();
    }

    Session session = (new ReadSessionFile(sessionFile.toString())).getSession();

    HashMap<String, SyntheticPKey> syntheticPKeys = new HashMap<String, SyntheticPKey>();
    syntheticPKeys.put("products", new SyntheticPKey("synth_id", 0));
    HashMap<String, NameAndCols> toSpanner = new HashMap<String, NameAndCols>();
    HashMap<String, String> cols = new HashMap<String, String>();
    cols.put("price", "price");
    cols.put("product_id", "product_id");
    toSpanner.put("products", new NameAndCols("products", cols));
    Session expectedSession = new Session(syntheticPKeys, toSpanner);

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
    Path sessionFile = Files.createTempFile("session-file-without-ToSpanner", ".json");
    Charset charset = Charset.forName("UTF-8");
    try (BufferedWriter writer = Files.newBufferedWriter(sessionFile, charset)) {
      String jsonString = getSessionStringWithoutToSpanner();
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

  private static String getSessionStringWithoutToSpanner() {
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

  private static String getSessionString() {
    return "{\n"
        + "    \"SpSchema\": {},\n"
        + "    \"SyntheticPKeys\": {\n"
        + "      \"products\": {\n"
        + "        \"Col\": \"synth_id\",\n"
        + "        \"Sequence\": 0\n"
        + "      }\n"
        + "    },\n"
        + "    \"SrcSchema\": {},\n"
        + "    \"Issues\": {\n"
        + "      \"products\": {}\n"
        + "    },\n"
        + "    \"ToSpanner\": {\n"
        + "      \"products\": {\n"
        + "        \"Name\": \"products\",\n"
        + "        \"Cols\": {\n"
        + "          \"price\": \"price\",\n"
        + "          \"product_id\": \"product_id\"\n"
        + "        }\n"
        + "      }\n"
        + "    },\n"
        + "    \"ToSource\": {},\n"
        + "    \"UsedNames\": {\n"
        + "      \"products\": true\n"
        + "    },\n"
        + "    \"Location\": {},\n"
        + "    \"Stats\": {\n"
        + "      \"Rows\": {\n"
        + "        \"products\": 3\n"
        + "      },\n"
        + "      \"GoodRows\": {},\n"
        + "      \"BadRows\": {},\n"
        + "      \"Statement\": {\n"
        + "        \"CreateTableStmt\": {\n"
        + "          \"Schema\": 1,\n"
        + "          \"Data\": 0,\n"
        + "          \"Skip\": 0,\n"
        + "          \"Error\": 0\n"
        + "        },\n"
        + "        \"DropTableStmt\": {\n"
        + "          \"Schema\": 0,\n"
        + "          \"Data\": 0,\n"
        + "          \"Skip\": 1,\n"
        + "          \"Error\": 0\n"
        + "        },\n"
        + "        \"InsertStmt\": {\n"
        + "          \"Schema\": 0,\n"
        + "          \"Data\": 1,\n"
        + "          \"Skip\": 0,\n"
        + "          \"Error\": 0\n"
        + "        }\n"
        + "      },\n"
        + "      \"Unexpected\": {},\n"
        + "      \"Reparsed\": 0\n"
        + "    },\n"
        + "    \"TimezoneOffset\": \"+00:00\",\n"
        + "    \"TargetDb\": \"spanner\",\n"
        + "    \"UniquePKey\": {},\n"
        + "    \"Audit\": {}\n"
        + "  }";
  }
}
