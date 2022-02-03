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

package erroru

import (
	"errors"
	"fmt"
	"testing"
)

func TestCombiningErrors(t *testing.T) {
	tests := []struct {
		original error
		err      error
		expected error
	}{
		{
			original: errors.New("first"),
			err:      errors.New("second"),
			expected: errors.New("first\nsecond"),
		},
		{
			original: nil,
			err:      errors.New("second"),
			expected: errors.New("second"),
		},
		{
			original: errors.New("first"),
			err:      nil,
			expected: errors.New("first"),
		},
		{
			original: nil,
			err:      nil,
			expected: nil,
		},
	}

	for _, test := range tests {
		if actual := CombineErrors(test.original, test.err); fmt.Sprint(actual) != fmt.Sprint(test.expected) {
			t.Errorf("Errors are not equal. Expected: %v. Got: %v.", test.expected, actual)
		}
	}
}
