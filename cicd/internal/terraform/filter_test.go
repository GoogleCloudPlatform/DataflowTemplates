/*
 * Copyright (C) 2023 Google LLC
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

package terraform

import (
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/zclconf/go-cty/cty"
	"testing"
)

func TestFilter_Apply(t *testing.T) {
	var matchFoo Matcher[*tfjson.SchemaAttribute] = MatchName[*tfjson.SchemaAttribute]("foo")

	tests := []struct {
		name    string
		input   map[string]*tfjson.SchemaAttribute
		matcher Matcher[*tfjson.SchemaAttribute]
		want    map[string]*tfjson.SchemaAttribute
	}{
		{
			name:    "empty",
			input:   map[string]*tfjson.SchemaAttribute{},
			matcher: And[*tfjson.SchemaAttribute]{},
			want:    map[string]*tfjson.SchemaAttribute{},
		},
		{
			name: "name matcher",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {},
				"bar": {},
			},
			matcher: matchFoo,
			want: map[string]*tfjson.SchemaAttribute{
				"foo": {},
			},
		},
		{
			name: "not(name) matcher",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {},
				"bar": {},
			},
			matcher: Not(matchFoo),
			want: map[string]*tfjson.SchemaAttribute{
				"bar": {},
			},
		},
		{
			name: "name and .Computed=true",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {
					Computed: true,
				},
				"bar": {
					Computed: true,
				},
			},
			matcher: And[*tfjson.SchemaAttribute]{
				matchFoo,
				MatchIsComputed(true),
			},
			want: map[string]*tfjson.SchemaAttribute{
				"foo": {
					Computed: true,
				},
			},
		},
		{
			name: "name or .Computed=true",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {
					Computed: true,
				},
				"bar": {
					Computed: true,
				},
			},
			matcher: Or[*tfjson.SchemaAttribute]{
				matchFoo,
				MatchIsComputed(true),
			},
			want: map[string]*tfjson.SchemaAttribute{
				"foo": {
					Computed: true,
				},
				"bar": {
					Computed: true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := &Filter[*tfjson.SchemaAttribute]{
				matcher: tt.matcher,
			}
			got := filter.Apply(tt.input)
			if diff := cmp.Diff(tt.want, got, cmpopts.EquateComparable(cty.Type{})); diff != "" {
				t.Errorf("Apply() mismatch (+want, -got)\n%s", diff)
			}
		})
	}
}
