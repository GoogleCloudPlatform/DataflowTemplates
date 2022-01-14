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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGetAllPomFiles(t *testing.T) {
	getModule := func(pomFile string) string {
		dir := filepath.Dir(pomFile)
		return dir[strings.LastIndex(dir, string(os.PathSeparator))+1:]
	}

	tests := []struct {
		dir      string
		expected map[string]interface{}
	}{
		{
			dir: FlexRoot,
			expected: map[string]interface{}{
				"v2":                         nil,
				"cdc-parent":                 nil,
				"cdc-common":                 nil,
				"pubsub-binary-to-bigquery":  nil,
				"googlecloud-to-googlecloud": nil,
			},
		},
		{
			dir: RootDirName,
			expected: map[string]interface{}{
				RootDirName:                  nil,
				"v2":                         nil,
				"cdc-parent":                 nil,
				"cdc-common":                 nil,
				"pubsub-binary-to-bigquery":  nil,
				"googlecloud-to-googlecloud": nil,
			},
		},
		{
			dir: ClassicRoot,
			expected: map[string]interface{}{
				RootDirName:                  nil,
				"v2":                         nil,
				"cdc-parent":                 nil,
				"cdc-common":                 nil,
				"pubsub-binary-to-bigquery":  nil,
				"googlecloud-to-googlecloud": nil,
			},
		},
	}

	for _, test := range tests {
		t.Logf("testing %s", test.dir)

		files, err := GetAllPomFiles(test.dir)
		if err != nil {
			t.Fatalf("error getting POM files: %v", err)
		}

		for _, f := range files {
			delete(test.expected, getModule(f))
		}

		if len(test.expected) > 0 {
			t.Fatalf("did not encounter %v. got %v", test.expected, files)
		}

		t.Logf("successful for %s", test.dir)
	}
}
