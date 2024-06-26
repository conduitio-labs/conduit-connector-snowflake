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
	"reflect"
)

type Table struct {
	Name     string
	Database string
	Schema   string
	Columns  []Column

	// Connector specific columns (all are also included in Columns)
	PrimaryKeys []Column
	Operation   Column
	CreatedAt   Column
	UpdatedAt   Column
	DeletedAt   Column
}

type Column struct {
	Name     string
	DataType DataType
}

type DataType interface {
	Nullable() bool
	Type() string
}

var (
	KnownDataTypes = map[string]DataType{
		DataTypeFixed{}.Type():        DataTypeFixed{},
		DataTypeReal{}.Type():         DataTypeReal{},
		DataTypeText{}.Type():         DataTypeText{},
		DataTypeBinary{}.Type():       DataTypeBinary{},
		DataTypeBoolean{}.Type():      DataTypeBoolean{},
		DataTypeDate{}.Type():         DataTypeDate{},
		DataTypeVariant{}.Type():      DataTypeVariant{},
		DataTypeTimestampLtz{}.Type(): DataTypeTimestampLtz{},
		DataTypeTimestampNtz{}.Type(): DataTypeTimestampNtz{},
		DataTypeTimestampTz{}.Type():  DataTypeTimestampTz{},
		DataTypeArray{}.Type():        DataTypeArray{},
		DataTypeObject{}.Type():       DataTypeObject{},
		DataTypeTime{}.Type():         DataTypeTime{},
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

type DataTypeReal struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeReal) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeReal) Type() string {
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

type DataTypeBoolean struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeBoolean) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeBoolean) Type() string {
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

type DataTypeVariant struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeVariant) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeVariant) Type() string {
	return "VARIANT"
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

type DataTypeArray struct {
	IsNullable bool `json:"nullable"`
}

func (dt DataTypeArray) Nullable() bool {
	return dt.IsNullable
}
func (dt DataTypeArray) Type() string {
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

// Container is a generic struct that can be used to unmarshal polymorphic JSON
// objects into a specific type based on a key. The key is used to determine the
// type of the object and the value is used to unmarshal the object into the
// correct type.
type Container[K comparable, V any, H ContainerHelper[K, V]] struct {
	Unmarshalled V
}

// ContainerHelper is an interface that must be implemented by the user to
// provide the key and the mapping function for the Container struct. The key
// is used to determine the type of the object and the mapping function is used
// to create a new instance of the object based on the key.
type ContainerHelper[K comparable, V any] interface {
	Key() K
	Mapping(K) V
}

func (c *Container[K, V, H]) UnmarshalJSON(b []byte) error {
	var helper H
	if err := json.Unmarshal(b, &helper); err != nil {
		return err
	}
	v := helper.Mapping(helper.Key())

	// Check if the value is a pointer of a value. If it's a pointer, we use it
	// as is. If it's a value, we create a pointer to it for the unmarshalling
	// to work and store the underlying value in the 'Unmarshalled' field.
	val := reflect.ValueOf(v)
	var ptrVal reflect.Value
	if val.Kind() != reflect.Ptr {
		// Create a new pointer type based on the type of 'v'.
		ptrType := reflect.PointerTo(val.Type())
		// Allocate a new object of this pointer type.
		ptrVal = reflect.New(ptrType.Elem())
		// Set the newly allocated object to the value of 'v'.
		ptrVal.Elem().Set(val)
		// Now 'ptrVal' is a reflect.Value of type '*V' which can be used as a pointer.
		v = ptrVal.Interface().(V)
	}

	if err := json.Unmarshal(b, v); err != nil {
		return err
	}

	if ptrVal.IsValid() {
		// If we used a pointer, we need to get the underlying value.
		c.Unmarshalled = ptrVal.Elem().Interface().(V)
	} else {
		// If we used the value directly, we store it in the 'Unmarshalled' field.
		c.Unmarshalled = v
	}

	return nil
}

// DataTypeContainer is a Container struct that can be used to unmarshal JSON
// objects into a DataType object based on the 'type' field in the JSON object.
type DataTypeContainer struct {
	Container[string, DataType, DataTypeContainerHelper]
}

// DataTypeContainerHelper is a struct that implements the ContainerHelper
// interface for the DataTypeContainer struct.
type DataTypeContainerHelper struct {
	Type string `json:"type"`
}

func (h DataTypeContainerHelper) Key() string {
	return h.Type
}

func (DataTypeContainerHelper) Mapping(t string) DataType {
	if dt, ok := KnownDataTypes[t]; ok {
		return dt
	}
	return DataTypeUnknown{TypeName: t}
}
