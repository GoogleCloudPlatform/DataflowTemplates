package hcl

import (
	"io"
)

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{
		w: w,
	}
}

type Encoder struct {
	w io.Writer
}
