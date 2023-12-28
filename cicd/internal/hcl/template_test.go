package hcl

import (
	"bytes"
	"errors"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	tfjson "github.com/hashicorp/terraform-json"
	"github.com/zclconf/go-cty/cty"
	"strings"
	"testing"
	"text/template"
	"time"
)

var (
	variableTestTmpl = template.Must(template.New("variable_template_test").Parse(`{{template "variable" .Attributes}}`))
)

func TestHeaderTemplate(t *testing.T) {
	buf := bytes.Buffer{}
	if err := headerTmpl.Execute(&buf, nil); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	want := strings.ReplaceAll(headerTmplF, "{{currentYear}}", time.Now().Format("2006"))
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("header.tmpl Execute() mismatch (-want,+got)\n%s", diff)
	}
}

func TestVariableTemplate(t *testing.T) {
	tests := []struct {
		name       string
		input      map[string]*tfjson.SchemaAttribute
		want       string
		exclusions []Matcher
		wantErr    bool
	}{
		{
			name: "zero value",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {},
			},
			want: `variable "foo"{


}`,
		},
		{
			name: "exclude by name",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {},
				"bar": {},
			},
			exclusions: []Matcher{
				NameMatcher("bar"),
			},
			want: `variable "foo"{


}`,
		},
		{
			name: "exclude deprecated",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {},
				"bar": {
					Deprecated: true,
				},
			},
			exclusions: []Matcher{
				MatchIsDeprecated,
			},
			want: `variable "foo"{


}`,
		},
		{
			name: "exclude computed",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {},
				"bar": {
					Computed: true,
				},
			},
			exclusions: []Matcher{
				MatchIsComputed,
			},
			want: `variable "foo" {


}`,
		},
		{
			name: "string type",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {
					AttributeType: cty.String,
				},
			},
			want: `variable "foo" {
	type = string

}`,
		},
		{
			name: "map(number) type",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {
					AttributeType: cty.Map(cty.Number),
				},
			},
			want: `variable "foo" {
	type = map(number)
}`,
		},
		{
			name: "with description",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {
					Description: "some description",
				},
			},
			want: `variable "foo" {

	description = "some description"
}
`,
		},
		{
			name: "2 variables",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {},
				"bar": {},
			},
			want: `
variable "bar" {
}

variable "foo" {
}
`,
			exclusions: nil,
			wantErr:    false,
		},
		{
			name: "optional",
			input: map[string]*tfjson.SchemaAttribute{
				"foo": {
					Optional: true,
				},
			},
			want: `
variable "foo" {
	default = null
}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := bytes.Buffer{}
			f := &templateFormatter{
				exclusions: tt.exclusions,
				tmpl:       variableTestTmpl,
			}
			input := &tfjson.SchemaBlock{
				Attributes: tt.input,
			}
			if err := f.variableTemplate().Execute(&buf, input); (err != nil) != tt.wantErr {
				t.Errorf("variableTemplate Execute() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			diff("variableTemplate Execute()", tt.want, buf, t)
		})
	}
}

func diff(message string, want string, got bytes.Buffer, t *testing.T) {
	wf, wd := hclsyntax.ParseConfig([]byte(want), "variable.tmpl", hcl.InitialPos)
	if wd.HasErrors() {
		t.Fatal(errors.Join(wd.Errs()...))
	}

	gf, gd := hclsyntax.ParseConfig(got.Bytes(), "variable.tmpl", hcl.InitialPos)
	if gd.HasErrors() {
		t.Fatal(errors.Join(gd.Errs()...))
	}

	opts := []cmp.Option{
		cmpopts.IgnoreUnexported(hclsyntax.Body{}, hclsyntax.ScopeTraversalExpr{}, hcl.TraverseRoot{}, hclsyntax.TemplateExpr{}, hclsyntax.LiteralValueExpr{}, cty.Value{}),
		cmpopts.IgnoreTypes(hcl.Range{}),
	}

	if diff := cmp.Diff(wf.Body, gf.Body, opts...); diff != "" {
		t.Errorf("%s mismatch (-want,+got)\n%s", message, diff)
	}
}
