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
	"sort"

	"github.com/go-errors/errors"
	"github.com/hamba/avro/v2"
)

const (
	schemaName = "record"
	schemaNS   = "io.conduitio.snowflake"
)

func NewAvroSchema(kcs KafkaConnectSchema) (avro.Schema, error) {
	var fields []*avro.Field

	for k, v := range kcs {
		f, err := avro.NewField(k, avro.NewPrimitiveSchema(schemaTypes[v], nil))
		if err != nil {
			return nil, errors.Errorf("failed to create new field %q, type %q: %w", k, v, err)
		}
		fields = append(fields, f)
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name() < fields[j].Name()
	})

	sch, err := avro.NewRecordSchema(schemaName, schemaNS, fields)
	if err != nil {
		return nil, errors.Errorf("failed to create avro schema: %w", err)
	}

	return sch, nil
}

func AvroFields(sch avro.Schema) map[string]avro.Type {
	var (
		rsch   = sch.(*avro.RecordSchema)
		fields = rsch.Fields()
		m      = make(map[string]avro.Type)
	)

	for _, f := range fields {
		psch := ((f.Type()).(*avro.PrimitiveSchema))
		m[f.Name()] = psch.Type()
	}

	return m
}
