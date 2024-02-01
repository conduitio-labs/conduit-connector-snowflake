package schema

import (
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/matryer/is"
)

type unsupported interface{}

func Test_Parse_UnsupportedTypes(t *testing.T) {
	v := []struct {
		kind  reflect.Kind
		value any
	}{
		{
			kind:  reflect.Uint,
			value: uint(10),
		},
		{
			kind:  reflect.Uint8,
			value: uint8(10),
		},
		{
			kind:  reflect.Uint16,
			value: uint16(10),
		},
		{
			kind:  reflect.Uint32,
			value: uint32(10),
		},
		{
			kind:  reflect.Uint64,
			value: uint64(10),
		},
		{
			kind:  reflect.Uintptr,
			value: uintptr(10),
		},
		{
			kind:  reflect.Map,
			value: map[string]string{},
		},
		{
			kind:  reflect.Slice,
			value: []string{},
		},
		{
			kind:  reflect.Array,
			value: [1]string{"a"},
		},
		{
			kind:  reflect.Struct,
			value: struct{}{},
		},
		{
			kind:  reflect.Pointer,
			value: &struct{}{},
		},
	}

	for _, tc := range v {
		t.Run(tc.kind.String(), func(t *testing.T) {
			is := is.New(t)

			_, err := Parse(sdk.Record{
				Payload: sdk.Change{
					After: sdk.StructuredData{
						tc.kind.String(): tc.value,
					},
				},
			})

			is.True(err != nil)
			expect := errors.Errorf("unsupported schema kind %q", tc.kind.String())
			is.Equal(err.Error(), expect.Error())
		})
	}
}

func Test_Parse(t *testing.T) {
	is := is.New(t)

	s, err := Parse(sdk.Record{
		Payload: sdk.Change{
			After: sdk.StructuredData{
				"string":  "foo",
				"int16":   int16(10),
				"int32":   int32(10),
				"bool":    true,
				"float32": float32(10.1),
				"float64": float64(10.2),
			},
		},
	})

	is.NoErr(err)
	is.Equal(s, Schema{
		"string":  reflect.String,
		"int16":   reflect.Int64,
		"int32":   reflect.Int64,
		"bool":    reflect.Bool,
		"float32": reflect.Float64,
		"float64": reflect.Float64,
	})
}
