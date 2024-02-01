// Copyright © 2022 Meroxa, Inc.
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

package format

import (
	"bytes"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Format defines the format the data will be persisted in by Destination
type Format string

const (
	// Parquet data format https://parquet.apache.org/
	CSV Format = "csv"
)

// All is a variable containing all supported format for enumeration
var All = []Format{
	CSV,
}

func Parse(name string) (Format, error) {
	switch name {
	case "csv":
		return CSV, nil
	default:
		return "", fmt.Errorf("unsupported format: %q", name)
	}
}

// MakeBytes returns a slice of bytes representing records in a given format
func (f Format) MakeBytes(records []sdk.Record, prefix string, indexColumns []string) (*bytes.Buffer, *bytes.Buffer, map[string]string, []string, []string, error) {
	switch f {
	case CSV:
		return makeCSVRecords(records, prefix, indexColumns)
	default:
		return nil, nil, nil, nil, nil, fmt.Errorf("unsupported format: %s", f)
	}
}
