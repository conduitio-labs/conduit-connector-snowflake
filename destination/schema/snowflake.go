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
)

func SnowflakeSchema(s Schema) map[string]string {
	snowschema := make(map[string]string)

	for k, v := range s {
		switch v {
		case reflect.Int64:
			// snowflake has single int type and they are all synonymous
			snowschema[k] = "BIGINT"
		case reflect.Bool:
			snowschema[k] = "BOOLEAN"
		case reflect.Float64:
			// similarly to ints, all floats are of equal width
			snowschema[k] = "FLOAT8"
		case reflect.String:
			// defaults to max string length when constraint is not specified
			snowschema[k] = "VARCHAR"
		case reflect.Invalid:
			// unknown data type.
			snowschema[k] = "VARIANT"
		}
	}

	return snowschema
}
