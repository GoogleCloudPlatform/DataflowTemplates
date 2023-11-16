/*
 * Copyright (C) 2018 Google Inc.
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
 * A good transform function.
 * @param {string} inJson
 * @return {string} outJson
 */
function transform(inJson) {
  var obj = JSON.parse(inJson);
  obj.someProp = "someValue";
  return JSON.stringify(obj);
}

/**
 * A transform function which only accepts 42 as the answer to life.
 * @param {string} inJson
 * @return {string} outJson
 */
function transformWithFilter(inJson) {
  var obj = JSON.parse(inJson);
  // only output objects which have an answer to life of 42.
  if (!(obj.hasOwnProperty('answerToLife') && obj.answerToLife != 42)) {
    return JSON.stringify(obj);
  }
}
/**
 * A transform function which copies data to another attribute, with computation in the middle.
 * @param {string} inJson
 * @return {string} outJson
 */
function transformSlow(inData) {

  var row = inData.split(',');
  var obj = new Object();

  obj.name = row[0];

  sum = 0;
  for (let i = 0; i <= 10000; i++) {
    sum += i;
  }
  obj.model = row[1];
  obj.sum = sum;

  return JSON.stringify(obj);
}