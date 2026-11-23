/*
 * Copyright (C) 2026 Google LLC
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

import "testing"

func TestSpecPathPrefix(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty",
			input:    "",
			expected: "",
		},
		{
			name:     "gcs_prefix",
			input:    "gs://dataflow-templates-staging/2026-11-24-00_RC00",
			expected: "-DspecPathPrefix=gs://dataflow-templates-staging/2026-11-24-00_RC00",
		},
	}

	orig := dSpecPathPrefix
	t.Cleanup(func() { dSpecPathPrefix = orig })

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dSpecPathPrefix = tc.input
			if got := SpecPathPrefix(); got != tc.expected {
				t.Errorf("SpecPathPrefix() = %q, want %q", got, tc.expected)
			}
		})
	}
}
