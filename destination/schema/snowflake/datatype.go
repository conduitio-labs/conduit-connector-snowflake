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

package snowflake

import (
	"encoding/json"
	"fmt"

	"github.com/lovromazgon/jsonpoly"
)

// DataType represents a Snowflake data type.
type DataType interface {
	Nullable() bool
	Type() string

	SQLType() string
}

var (
	KnownDataTypes = map[string]DataType{
		DataTypeFixed{}.Type():        DataTypeFixed{},
		DataTypeReal{}.Type():         DataTypeReal{},
		DataTypeText{}.Type():         DataTypeText{},
		DataTypeBinary{}.Type():       DataTypeBinary{},
		DataTypeBoolean{}.Type():      DataTypeBoolean{},
		DataTypeDate{}.Type():         DataTypeDate{},
		DataTypeTime{}.Type():         DataTypeTime{},
		DataTypeTimestampLtz{}.Type(): DataTypeTimestampLtz{},
		DataTypeTimestampNtz{}.Type(): DataTypeTimestampNtz{},
		DataTypeTimestampTz{}.Type():  DataTypeTimestampTz{},
		DataTypeVariant{}.Type():      DataTypeVariant{},
		DataTypeArray{}.Type():        DataTypeArray{},
		DataTypeObject{}.Type():       DataTypeObject{},
		DataTypeGeography{}.Type():    DataTypeGeography{},
		DataTypeVector{}.Type():       DataTypeVector{},
	}
)

type DataTypeFixed struct {
	IsNullable bool `json:"nullable"`
	Precision  int  `json:"precision"`
	Scale      int  `json:"scale"`
}

func (dt DataTypeFixed) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeFixed) Type() string {
	return "FIXED"
}
func (dt DataTypeFixed) SQLType() string {
	if dt.Precision > 0 || dt.Scale > 0 {
		return fmt.Sprintf("NUMBER(%d,%d)", dt.Precision, dt.Scale)
	}
	return "NUMBER"
}

type DataTypeReal struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeReal) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeReal) Type() string {
	return "REAL"
}
func (dt DataTypeReal) SQLType() string {
	return "REAL"
}

type DataTypeText struct {
	IsNullable bool  `json:"nullable"`
	Length     int64 `json:"length"`
	ByteLength int64 `json:"byteLength"`
	Fixed      bool  `json:"fixed"`
}

func (dt DataTypeText) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeText) Type() string {
	return "TEXT"
}
func (dt DataTypeText) SQLType() string {
	if dt.Length > 0 {
		return fmt.Sprintf("TEXT(%d)", dt.Length)
	}
	return "TEXT"
}

type DataTypeBinary struct {
	IsNullable bool  `json:"nullable"`
	Length     int64 `json:"length"`
	ByteLength int64 `json:"byteLength"`
	Fixed      bool  `json:"fixed"`
}

func (dt DataTypeBinary) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeBinary) Type() string {
	return "BINARY"
}
func (dt DataTypeBinary) SQLType() string {
	return "BINARY"
}

type DataTypeBoolean struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeBoolean) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeBoolean) Type() string {
	return "BOOLEAN"
}
func (dt DataTypeBoolean) SQLType() string {
	return "BOOLEAN"
}

type DataTypeDate struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeDate) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeDate) Type() string {
	return "DATE"
}
func (dt DataTypeDate) SQLType() string {
	return "DATE"
}

type DataTypeTime struct {
	IsNullable bool  `json:"nullable"`
	Precision  int64 `json:"precision"`
	Scale      int64 `json:"scale"`
}

func (dt DataTypeTime) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeTime) Type() string {
	return "TIME"
}
func (dt DataTypeTime) SQLType() string {
	if dt.Scale > 0 {
		return fmt.Sprintf("TIME(%d)", dt.Scale)
	}
	return "TIME"
}

type DataTypeTimestampLtz struct {
	IsNullable bool  `json:"nullable"`
	Precision  int64 `json:"precision"`
	Scale      int64 `json:"scale"`
}

func (dt DataTypeTimestampLtz) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeTimestampLtz) Type() string {
	return "TIMESTAMP_LTZ"
}
func (dt DataTypeTimestampLtz) SQLType() string {
	if dt.Scale > 0 {
		return fmt.Sprintf("TIMESTAMP_LTZ(%d)", dt.Scale)
	}
	return "TIMESTAMP_LTZ"
}

type DataTypeTimestampNtz struct {
	IsNullable bool  `json:"nullable"`
	Precision  int64 `json:"precision"`
	Scale      int64 `json:"scale"`
}

func (dt DataTypeTimestampNtz) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeTimestampNtz) Type() string {
	return "TIMESTAMP_NTZ"
}
func (dt DataTypeTimestampNtz) SQLType() string {
	if dt.Scale > 0 {
		return fmt.Sprintf("TIMESTAMP_NTZ(%d)", dt.Scale)
	}
	return "TIMESTAMP_NTZ"
}

type DataTypeTimestampTz struct {
	IsNullable bool  `json:"nullable"`
	Precision  int64 `json:"precision"`
	Scale      int64 `json:"scale"`
}

func (dt DataTypeTimestampTz) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeTimestampTz) Type() string {
	return "TIMESTAMP_TZ"
}
func (dt DataTypeTimestampTz) SQLType() string {
	if dt.Scale > 0 {
		return fmt.Sprintf("TIMESTAMP_TZ(%d)", dt.Scale)
	}
	return "TIMESTAMP_TZ"
}

type DataTypeVariant struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeVariant) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeVariant) Type() string {
	return "VARIANT"
}
func (dt DataTypeVariant) SQLType() string {
	return "VARIANT"
}

type DataTypeArray struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeArray) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeArray) Type() string {
	return "ARRAY"
}
func (dt DataTypeArray) SQLType() string {
	return "ARRAY"
}

type DataTypeObject struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeObject) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeObject) Type() string {
	return "OBJECT"
}
func (dt DataTypeObject) SQLType() string {
	return "OBJECT"
}

type DataTypeGeography struct {
	IsNullable bool   `json:"nullable"`
	OutputType string `json:"outputType"`
}

func (dt DataTypeGeography) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeGeography) Type() string {
	return "GEOGRAPHY"
}
func (dt DataTypeGeography) SQLType() string {
	return "GEOGRAPHY"
}

type DataTypeVector struct {
	IsNullable  bool     `json:"nullable"`
	Dimension   int64    `json:"dimension"`
	ElementType DataType `json:"vectorElementType"`
}

func (dt DataTypeVector) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeVector) Type() string {
	return "VECTOR"
}
func (dt DataTypeVector) SQLType() string {
	return fmt.Sprintf("VECTOR(%s,%d)", dt.ElementType.SQLType(), dt.Dimension)
}
func (dt *DataTypeVector) UnmarshalJSON(b []byte) error {
	// Create a type alias to avoid infinite recursion when calling json.Unmarshal.
	type Alias DataTypeVector
	tmp := struct {
		*Alias
		ElementType DataTypeContainer `json:"vectorElementType"`
	}{
		Alias: (*Alias)(dt),
	}

	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	dt.ElementType = tmp.ElementType.Value
	return nil
}

type DataTypeUnknown struct {
	TypeName   string          `json:"type"`
	IsNullable bool            `json:"nullable"`
	Raw        json.RawMessage `json:"-"`
}

func (dt DataTypeUnknown) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeUnknown) Type() string {
	return dt.TypeName
}
func (dt DataTypeUnknown) SQLType() string {
	return dt.TypeName
}
func (dt *DataTypeUnknown) UnmarshalJSON(b []byte) error {
	// Create a type alias to avoid infinite recursion when calling json.Unmarshal.
	type Alias DataTypeUnknown

	// Use the alias to unmarshal the known fields.
	aux := &struct{ *Alias }{Alias: (*Alias)(dt)}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	// Store the raw JSON in the end.
	dt.Raw = append(dt.Raw[:0], b...)
	return nil
}

// -----------------------------------------------------------------------------

// DataTypeContainer is a Container struct that can be used to unmarshal JSON
// objects into a DataType object based on the 'type' field in the JSON object.
type DataTypeContainer struct {
	jsonpoly.Container[DataType, *dataTypeContainerHelper]
}

// dataTypeContainerHelper is a struct that implements the ContainerHelper
// interface for the DataTypeContainer struct.
type dataTypeContainerHelper struct {
	Type string `json:"type"`
}

func (h *dataTypeContainerHelper) Get() DataType {
	if dt, ok := KnownDataTypes[h.Type]; ok {
		return dt
	}
	return DataTypeUnknown{TypeName: h.Type}
}

func (h *dataTypeContainerHelper) Set(t DataType) {
	h.Type = t.Type()
}
