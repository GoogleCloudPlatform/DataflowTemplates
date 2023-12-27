package hcl

import (
	"bytes"
	"github.com/google/go-cmp/cmp"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/zclconf/go-cty/cty"
	"strings"
	"testing"
)

func TestTemplate_header(t *testing.T) {
	want := strings.ReplaceAll(headerTmplF, "{{currentYear}}", currentYear())
	buf := bytes.Buffer{}
	if err := headerTmpl.Execute(&buf, ""); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(want, buf.String()); diff != "" {
		t.Errorf("headerTmpl.Execute() mismatch (+want,-got)\n%s", diff)
	}
}

func TestTemplate_variable(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input map[string]*tfjson.SchemaAttribute
		want  string
	}{
		{
			name: "",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {
					AttributeType: cty.String,
				},
			},
			want: `variable "foo" {
  type = string
}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			if err := variableTmpl.Execute(&buf, tt.input); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, buf.String()); diff != "" {
				t.Errorf("variableTmpl.Execute() mismatch (-want,+got)\n%s", diff)
			}
		})
	}
}
