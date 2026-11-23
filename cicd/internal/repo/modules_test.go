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

package repo

import (
	"reflect"
	"testing"
)

func TestModuleMappingHasAllRoots(t *testing.T) {
	m := getModuleMapping()
	if _, ok := m[ClassicRoot]; !ok {
		t.Error("Missing Classic root")
	}
	if _, ok := m[ProjectRoot]; !ok {
		t.Error("Missing Project root")
	}
	if _, ok := m[ItRoot]; !ok {
		t.Error("Missing integration test root")
	}
	if _, ok := m[FlexRoot]; !ok {
		t.Error("Missing Flex root")
	}
}

func TestGetModulesForPaths(t *testing.T) {
	tests := []struct {
		input    []string
		expected map[string][]string
	}{
		{
			input: []string{
				"src/something",
				"it/something",
				"v2/googlecloud-to-googlecloud/avro",
				"src/something-else",
				"v2/googlecloud-to-googlecloud/proto",
				"v2/something",
			},
			expected: map[string][]string{
				ProjectRoot: []string{},
				ItRoot:      []string{},
				FlexRoot:    []string{"googlecloud-to-googlecloud", ""},
			},
		},
		{
			input: []string{"v2/dataplex", "v2/googlecloud-to-googlecloud"},
			expected: map[string][]string{
				FlexRoot: []string{"dataplex", "googlecloud-to-googlecloud"},
			},
		},
		{
			input: []string{"something", "it/something", "v2/something"},
			expected: map[string][]string{
				ProjectRoot: make([]string, 0),
				ItRoot:      make([]string, 0),
				FlexRoot:    []string{""},
			},
		},
		{
			input: []string{"something", "it/something", "v1/something"},
			expected: map[string][]string{
				ProjectRoot: make([]string, 0),
				ItRoot:      make([]string, 0),
				ClassicRoot: make([]string, 0),
			},
		},
		{
			input:    make([]string, 0),
			expected: make(map[string][]string),
		},
	}

	for _, test := range tests {
		t.Logf("Testing input: %v", test.input)
		if actual := GetModulesForPaths(test.input); !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("incorrect results. expected: %v. got: %v", test.expected, actual)
		}
		t.Logf("Success")
	}
}
