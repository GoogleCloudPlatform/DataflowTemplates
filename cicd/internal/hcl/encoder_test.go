package hcl

import (
	tfjson "github.com/hashicorp/terraform-json"
	"io"
	"testing"
)

func TestEncoder_Encode(t *testing.T) {
	type fields struct {
		w         io.Writer
		formatter Formatter
	}
	type args struct {
		schema *tfjson.ProviderSchema
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := &Encoder{
				w:         tt.fields.w,
				formatter: tt.fields.formatter,
			}
			if err := enc.Encode(tt.args.schema); (err != nil) != tt.wantErr {
				t.Errorf("Encode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
