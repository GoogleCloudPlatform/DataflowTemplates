/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * This function splits a csv and turns it into a pre-defined JSON.
 * @param {string} line is a line from TextIO.
 * @return {JSON} a JSON created after parsing the line.
 */
function transform(line) {
  try {
    var split = line.split(",");
    var obj = new Object();
    obj.id = split[0];
    obj.state = split[1];
    obj.price = parseFloat(split[2]);
    var jsonString = JSON.stringify(obj);
    return jsonString;

  } catch (e) {
    return false;
  }

}
