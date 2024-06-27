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
	"slices"

	"github.com/go-errors/errors"
	"golang.org/x/exp/maps"
)

func init() {
	keys := maps.Keys(snowflakeTypes)
	values := maps.Values(schemaTypes)

	slices.Sort(keys)
	slices.Sort(values)

	if slices.Compare(keys, values) != 0 {
		panic("schema types mismatch")
	}
}

type Schema map[string]reflect.Kind

var schemaTypes = map[string]reflect.Kind{
	"INT":     reflect.Int,
	"INT8":    reflect.Int8,
	"INT16":   reflect.Int16,
	"INT32":   reflect.Int32,
	"INT64":   reflect.Int64,
	"STRING":  reflect.String,
	"BOOLEAN": reflect.Bool,
	"FLOAT32": reflect.Float32,
	"FLOAT64": reflect.Float64,
}

func New(kcs KafkaConnectSchema) (Schema, error) {
	sch := make(Schema)

	for k, v := range kcs {
		t, ok := schemaTypes[v]
		if !ok {
			return nil, errors.Errorf("type %q missing mapping", v)
		}
		sch[k] = t
	}

	return sch, nil
}
