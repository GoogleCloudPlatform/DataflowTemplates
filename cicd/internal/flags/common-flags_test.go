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
)

func TestChangedFiles(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			input:    "file1,file2",
			expected: []string{"file1", "file2"},
		},
		{
			input:    "file1",
			expected: []string{"file1"},
		},
	}

	for _, test := range tests {
		changedFiles = test.input
		actual := ChangedFiles()
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("Returned files are not equal. Expected %v. Got %v.", test.expected, actual)
		}
	}
}

func TestChangedFilesEmpty(t *testing.T) {
	changedFiles = ""
	actual := ChangedFiles()
	if len(actual) != 0 {
		t.Errorf("Expected empty slice, but got %v of len %v", actual, len(actual))
	}
}
