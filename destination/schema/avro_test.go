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
	_ "embed"
	"sort"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/matryer/is"
)

//go:embed testdata/avro-schema.json
var testAvroSchema string

func Test_NewAvroSchema(t *testing.T) {
	is := is.New(t)

	ksc, err := ParseKafkaConnect(testKafaConnectSchema)
	is.NoErr(err)

	asch, err := NewAvroSchema(ksc)
	is.NoErr(err)

	want, err := avro.Parse(testAvroSchema)
	is.NoErr(err)
	sortSchema(t, want)

	is.Equal(want.String(), asch.String())
}

func sortSchema(t *testing.T, s avro.Schema) {
	t.Helper()
	is := is.New(t)

	rs, ok := s.(*avro.RecordSchema)
	is.True(ok)

	fields := rs.Fields()
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name() < fields[j].Name()
	})
}
