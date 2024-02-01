package schema

import (
	"reflect"
	"sort"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
)

type Schema map[string]reflect.Kind

func Parse(r sdk.Record) (Schema, error) {
	schema := make(Schema)

	switch t := r.Payload.After.(type) {
	case sdk.StructuredData:
	case sdk.RawData:
		return nil, errors.New("raw data payload is unsupported")
	default:
		return nil, errors.Errorf("unsupported payload type %q", t)
	}

	for k, v := range r.Payload.After.(sdk.StructuredData) {
		switch kind := reflect.ValueOf(v).Kind(); kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			schema[k] = reflect.Int64
		case reflect.Bool:
			schema[k] = reflect.Bool
		case reflect.Float32, reflect.Float64:
			schema[k] = reflect.Float64
		case reflect.String:
			schema[k] = reflect.String
		default:
			return nil, errors.Errorf("unsupported schema kind %q", kind.String())
		}
	}

	return schema, nil
}

func (s Schema) OrderedFields() []string {
	fields := make([]string, 0, len(s))
	for k, _ := range s {
		fields = append(fields, k)
	}

	sort.Strings(fields)

	return fields
}