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

package writer

import (
	"encoding/json"
	"fmt"
	"github.com/matryer/is"
	"testing"
)

func TestDataTypeContainer_JSONUnmarshal(t *testing.T) {
	testCases := []struct {
		have string
		want DataType
	}{{
		have: `{"type":"FIXED","precision":38,"scale":0,"nullable":true}`,
		want: DataTypeFixed{
			IsNullable: true,
			Precision:  38,
			Scale:      0,
		},
	}, {
		have: `{"type":"REAL","nullable":true}`,
		want: DataTypeReal{
			IsNullable: true,
		},
	}, {
		have: `{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}`,
		want: DataTypeText{
			IsNullable: true,
			Length:     16777216,
			ByteLength: 16777216,
			Fixed:      false,
		},
	}, {
		have: `{"type":"BINARY","length":8388608,"byteLength":8388608,"nullable":true,"fixed":true}`,
		want: DataTypeBinary{
			IsNullable: true,
			Length:     8388608,
			ByteLength: 8388608,
			Fixed:      true,
		},
	}, {
		have: `{"type":"BOOLEAN","nullable":true}`,
		want: DataTypeBoolean{
			IsNullable: true,
		},
	}, {
		have: `{"type":"DATE","nullable":true}`,
		want: DataTypeDate{
			IsNullable: true,
		},
	}, {
		have: `{"type":"TIME","precision":0,"scale":9,"nullable":true}`,
		want: DataTypeTime{
			IsNullable: true,
			Precision:  0,
			Scale:      9,
		},
	}, {
		have: `{"type":"TIMESTAMP_LTZ","precision":0,"scale":9,"nullable":true}`,
		want: DataTypeTimestampLtz{
			IsNullable: true,
			Precision:  0,
			Scale:      9,
		},
	}, {
		have: `{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":true}`,
		want: DataTypeTimestampNtz{
			IsNullable: true,
			Precision:  0,
			Scale:      9,
		},
	}, {
		have: `{"type":"TIMESTAMP_TZ","precision":0,"scale":9,"nullable":true}`,
		want: DataTypeTimestampTz{
			IsNullable: true,
			Precision:  0,
			Scale:      9,
		},
	}, {
		have: `{"type":"FOO","foo":"bar","nullable":true}`,
		want: DataTypeUnknown{
			IsNullable: true,
			TypeName:   "FOO",
			Raw:        json.RawMessage(`{"type":"FOO","foo":"bar","nullable":true}`),
		},
	}}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%T", tc.want), func(t *testing.T) {
			is := is.New(t)
			var dt DataTypeContainer
			err := json.Unmarshal([]byte(tc.have), &dt)
			is.NoErr(err)

			got := dt.Unmarshalled
			is.Equal(got, tc.want)
		})
	}
}
