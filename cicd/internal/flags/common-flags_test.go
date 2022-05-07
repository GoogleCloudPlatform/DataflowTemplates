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

func TestChangedFilesNoRegex(t *testing.T) {
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

func TestChangedFilesNoRegexEmpty(t *testing.T) {
	changedFiles = ""
	actual := ChangedFiles()
	if len(actual) != 0 {
		t.Errorf("Expected empty slice, but got %v of len %v", actual, len(actual))
	}
}

func TestChangedFilesRegexes(t *testing.T) {
	tests := []struct {
		files    string
		regexes  []string
		expected []string
	}{
		{
			files:    "file1,file2,file3",
			regexes:  []string{"file[1|3]"},
			expected: []string{"file1", "file3"},
		},
		{
			files:    "file1,file2,file3",
			regexes:  []string{"f.+1", "fi.+3"},
			expected: []string{"file1", "file3"},
		},
		{
			files:    "file1,file2,fileN",
			regexes:  []string{"\\d"},
			expected: []string{"file1", "file2"},
		},
		{
			files:    "foo.c,bar.cc",
			regexes:  []string{"\\.c$"},
			expected: []string{"foo.c"},
		},
	}

	for _, test := range tests {
		changedFiles = test.files
		actual := ChangedFiles(test.regexes...)
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("Returned files are not equal. Expected %v. Got %v.", test.expected, actual)
		}
	}
}

func TestChangedFilesRegexesNoMatch(t *testing.T) {
	changedFiles = "foo,bar"
	actual := ChangedFiles("file")
	if len(actual) != 0 {
		t.Errorf("Expected empty slice but got %v", actual)
	}
}
