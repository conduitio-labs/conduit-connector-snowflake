// Copyright © 2024 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		case reflect.Invalid:
			schema[k] = reflect.Invalid // this designates null value type
		default:
			return nil, errors.Errorf("unsupported schema kind %q", kind.String())
		}
	}

	return schema, nil
}

func (s Schema) OrderedFields() []string {
	fields := make([]string, 0, len(s))
	for k := range s {
		fields = append(fields, k)
	}

	sort.Strings(fields)

	return fields
}
