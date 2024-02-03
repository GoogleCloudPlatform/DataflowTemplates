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
	tfjson "github.com/hashicorp/terraform-json"
	"testing"
)

func TestAnd_Match(t *testing.T) {
	type args struct {
		name string
		data *tfjson.SchemaAttribute
	}
	tests := []struct {
		name    string
		args    args
		matcher And[*tfjson.SchemaAttribute]
		want    bool
	}{
		{
			name: "And[] empty",
			args: args{},
			want: true,
		},
		{
			name: "And{true}",
			args: args{},
			matcher: And[*tfjson.SchemaAttribute]{
				&matchTrue[*tfjson.SchemaAttribute]{},
			},
			want: true,
		},
		{
			name: "And{false}",
			args: args{},
			matcher: And[*tfjson.SchemaAttribute]{
				&matchFalse[*tfjson.SchemaAttribute]{},
			},
			want: false,
		},
		{
			name: "And{true,false}",
			args: args{},
			matcher: And[*tfjson.SchemaAttribute]{
				&matchTrue[*tfjson.SchemaAttribute]{},
				&matchFalse[*tfjson.SchemaAttribute]{},
			},
			want: false,
		},
		{
			name: "And{false,true}",
			args: args{},
			matcher: And[*tfjson.SchemaAttribute]{
				&matchFalse[*tfjson.SchemaAttribute]{},
				&matchTrue[*tfjson.SchemaAttribute]{},
			},
			want: false,
		},
		{
			name: "And{false,false}",
			args: args{},
			matcher: And[*tfjson.SchemaAttribute]{
				&matchFalse[*tfjson.SchemaAttribute]{},
				&matchFalse[*tfjson.SchemaAttribute]{},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.want != tt.matcher.Match(tt.args.name, tt.args.data) {
				t.Errorf("Match() = %v, want %v", !tt.want, tt.want)
			}
		})
	}
}

func TestOr_Match(t *testing.T) {
	type args struct {
		name string
		data *tfjson.SchemaAttribute
	}
	tests := []struct {
		name    string
		args    args
		matcher Or[*tfjson.SchemaAttribute]
		want    bool
	}{
		{
			name: "Or[] empty",
			args: args{},
			want: true,
		},
		{
			name: "Or{true}",
			args: args{},
			matcher: Or[*tfjson.SchemaAttribute]{
				&matchTrue[*tfjson.SchemaAttribute]{},
			},
			want: true,
		},
		{
			name: "Or{false}",
			args: args{},
			matcher: Or[*tfjson.SchemaAttribute]{
				&matchFalse[*tfjson.SchemaAttribute]{},
			},
			want: false,
		},
		{
			name: "Or{true,false}",
			args: args{},
			matcher: Or[*tfjson.SchemaAttribute]{
				&matchTrue[*tfjson.SchemaAttribute]{},
				&matchFalse[*tfjson.SchemaAttribute]{},
			},
			want: true,
		},
		{
			name: "Or{false,true}",
			args: args{},
			matcher: Or[*tfjson.SchemaAttribute]{
				&matchFalse[*tfjson.SchemaAttribute]{},
				&matchTrue[*tfjson.SchemaAttribute]{},
			},
			want: true,
		},
		{
			name: "Or{false,false}",
			args: args{},
			matcher: Or[*tfjson.SchemaAttribute]{
				&matchFalse[*tfjson.SchemaAttribute]{},
				&matchFalse[*tfjson.SchemaAttribute]{},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.want != tt.matcher.Match(tt.args.name, tt.args.data) {
				t.Errorf("Match() = %v, want %v", !tt.want, tt.want)
			}
		})
	}
}

func TestMatchIsComputed_Match(t *testing.T) {
	type args struct {
		name string
		data *tfjson.SchemaAttribute
	}
	tests := []struct {
		name    string
		args    args
		matcher MatchIsComputed
		want    bool
	}{
		{
			name: "nil data",
			args: args{},
			want: false,
		},
		{
			name: "matcher=false,.Computed=false",
			args: args{
				data: &tfjson.SchemaAttribute{
					Computed: false,
				},
			},
			matcher: false,
			want:    true,
		},
		{
			name: "matcher=true,.Computed=false",
			args: args{
				data: &tfjson.SchemaAttribute{
					Computed: false,
				},
			},
			matcher: true,
			want:    false,
		},
		{
			name: "matcher=false,.Computed=true",
			args: args{
				data: &tfjson.SchemaAttribute{
					Computed: true,
				},
			},
			matcher: false,
			want:    false,
		},
		{
			name: "matcher=true,.Computed=true",
			args: args{
				data: &tfjson.SchemaAttribute{
					Computed: true,
				},
			},
			matcher: true,
			want:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.want != tt.matcher.Match(tt.args.name, tt.args.data) {
				t.Errorf("Match() = %v, want %v", !tt.want, tt.want)
			}
		})
	}
}

type matchTrue[S Schema] struct{}

func (matcher *matchTrue[S]) Match(_ string, _ S) bool {
	return true
}

type matchFalse[S Schema] struct{}

func (matcher *matchFalse[S]) Match(_ string, _ S) bool {
	return false
}

func TestNot(t *testing.T) {
	type testCase[S Schema] struct {
		name    string
		matcher Matcher[S]
		want    bool
	}
	tests := []testCase[*tfjson.ProviderSchema]{
		{
			name:    "not(true)",
			matcher: &matchTrue[*tfjson.ProviderSchema]{},
			want:    false,
		},
		{
			name:    "not(false)",
			matcher: &matchFalse[*tfjson.ProviderSchema]{},
			want:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Not(tt.matcher).Match("", nil)
			if tt.want != got {
				t.Errorf("Not(%T) = %v, want %v", tt.matcher, got, tt.want)
			}
		})
	}
}
