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
	"encoding/json"
	"slices"
	"strings"

	"github.com/go-errors/errors"
	"github.com/hamba/avro/v2"
	"golang.org/x/exp/maps"
)

var schemaTypes = map[string]avro.Type{
	"INT":     avro.Int,
	"INT8":    avro.Int,
	"INT16":   avro.Int,
	"INT32":   avro.Int,
	"INT64":   avro.Long,
	"STRING":  avro.String,
	"BOOLEAN": avro.Boolean,
	"FLOAT64": avro.Double,
	"FLOAT32": avro.Double,
}

type (
	KafkaConnectSchema map[string]string
	AvroSchema         map[string]any
)

// ParseKafkaConnect returns a parsed Kafka Connect Schema from JSON string.
// Returns error when JSON decoding or validation fail.
func ParseKafkaConnect(s string) (KafkaConnectSchema, error) {
	kcs := make(KafkaConnectSchema)

	if err := json.Unmarshal([]byte(s), &kcs); err != nil {
		return nil, errors.Errorf("failed to parse kafka connect schema: %w", err)
	}

	if err := kcs.Validate(); err != nil {
		return nil, errors.Errorf("failed to validate schema: %w", err)
	}

	return kcs, nil
}

// Validate returns an error when the schema contains unsupported types.
func (ksc KafkaConnectSchema) Validate() error {
	var invalid []string

	validTypes := maps.Keys(schemaTypes)

	for _, v := range ksc {
		if !slices.Contains(validTypes, strings.ToUpper(v)) {
			invalid = append(invalid, v)
		}
	}

	if c := len(invalid); c != 0 {
		return errors.Errorf("found %d unsupported types: %s", c, strings.Join(invalid, ", "))
	}

	return nil
}

func (ksc KafkaConnectSchema) ToAvro() AvroSchema {
	var fields []map[string]string

	for k, v := range ksc {
		fields = append(fields, map[string]string{
			"name": strings.ToLower(k),
			"type": string(schemaTypes[strings.ToUpper(v)]),
		})
	}

	return AvroSchema{
		"type":      avro.Record,
		"name":      "snowflakerow",
		"namespace": "io.conduitio.snowflake",
		"fields":    fields,
	}
}
