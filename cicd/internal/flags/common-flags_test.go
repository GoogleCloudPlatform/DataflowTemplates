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

package flags

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestModulesToBuild(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			input:    "m1,m2",
			expected: []string{"m1", "m2"},
		},
		{
			input:    "m1",
			expected: []string{"m1"},
		},
		{
			input:    "ALL",
			expected: []string{},
		},
		{
			input:    "KAFKA",
			expected: []string{"v2/kafka-common/", "v2/kafka-to-bigquery/", "v2/kafka-to-gcs/", "v2/kafka-to-kafka/", "v2/kafka-to-pubsub/", "plugins/templates-maven-plugin"},
		},
		{
			input:    "SPANNER",
			expected: []string{"v2/datastream-to-spanner/", "v2/spanner-change-streams-to-sharded-file-sink/", "v2/gcs-to-sourcedb/", "v2/sourcedb-to-spanner/", "v2/spanner-to-sourcedb/", "v2/spanner-custom-shard", "plugins/templates-maven-plugin"},
		},
	}

	for _, test := range tests {
		modulesToBuild = test.input
		actual := ModulesToBuild()
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("Returned modules are not equal. Expected %v. Got %v.", test.expected, actual)
		}
	}
}

func TestDefaultExcludedSubModules(t *testing.T) {
	// common modules won't excluded
	modulesToBuild = "DEFAULT"
	defaults := ModulesToBuild()
	// these are modules appended to moduleMap
	excluded := map[string]int{"plugins/templates-maven-plugin": 0, "metadata/": 0, "v2/kafka-common/": 0}
	var s []string
	for m, _ := range moduleMap {
		if m == "ALL" || m == "DEFAULT" {
			continue
		}
		modulesToBuild = m
		ms := ModulesToBuild()
		for _, n := range ms {
			if _, ok := excluded[n]; !ok {
				s = append(s, "!"+n)
			}
		}
	}
	less := func(a, b string) bool { return a < b }
	if "" != cmp.Diff(defaults, s, cmpopts.SortSlices(less)) {
		t.Errorf("Returned modules are not equal. Expected %v. Got %v.", s, defaults)
	}
}
