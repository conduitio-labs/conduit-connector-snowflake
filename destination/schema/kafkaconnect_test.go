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
	"testing"

	"github.com/matryer/is"
)

//go:embed testdata/kafka-connect-schema.json
var testKafaConnectSchema string

func Test_ParseKafkaConnect(t *testing.T) {
	is := is.New(t)

	ksc, err := ParseKafkaConnect(testKafaConnectSchema)
	is.NoErr(err)
	is.Equal(ksc, KafkaConnectSchema{
		"category":         "STRING",
		"customer_email":   "STRING",
		"id":               "INT64",
		"product_id":       "INT64",
		"product_name":     "STRING",
		"product_type":     "STRING",
		"shipping_address": "STRING",
		"stock":            "BOOLEAN",
	})
}
