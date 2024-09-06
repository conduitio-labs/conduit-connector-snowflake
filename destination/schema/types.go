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
	"github.com/go-errors/errors"
)

type Type string

// Data types in Snowflake.
const (
	Number          Type = "NUMBER"
	Decimal         Type = "DECIMAL"
	Numeric         Type = "NUMERIC"
	Int             Type = "INT"
	Integer         Type = "INTEGER"
	Bigint          Type = "BIGINT"
	Smallint        Type = "SMALLINT"
	Tinyint         Type = "TINYINT"
	Byteint         Type = "BYTEINT"
	Float           Type = "FLOAT"
	Float4          Type = "FLOAT4"
	Float8          Type = "FLOAT8"
	Double          Type = "DOUBLE"
	DoublePrecision Type = "DOUBLE PRECISION"
	Real            Type = "REAL"
	Varchar         Type = "VARCHAR"
	Char            Type = "CHAR"
	Character       Type = "CHARACTER"
	String          Type = "STRING"
	Text            Type = "TEXT"
	Binary          Type = "BINARY"
	Varbinary       Type = "VARBINARY"
	Boolean         Type = "BOOLEAN"
	Date            Type = "DATE"
	DateTime        Type = "DATETIME"
	Time            Type = "TIME"
	Timestamp       Type = "TIMESTAMP"
	TimestampLTZ    Type = "TIMESTAMP_LTZ"
	TimestampNTZ    Type = "TIMESTAMP_NTZ"
	TimestampTZ     Type = "TIMESTAMP_TZ"
	Variant         Type = "VARIANT"
	Object          Type = "OBJECT"
	Array           Type = "ARRAY"
	Geography       Type = "GEOGRAPHY"
	Geometry        Type = "GEOMETRY"
	Vector          Type = "VECTOR"
)

// Unalias returns the main type.
// Returns self if the does not have any alias.
func (t Type) Unalias() Type {
	switch t {
	case Int, Integer, Bigint, Smallint, Tinyint, Byteint:
		return Integer
	case Numeric, Decimal:
		return Number
	case Float, Float4, Float8, Double, DoublePrecision, Real:
		return Float
	case Varchar, Char, Character, String, Text:
		return Varchar // defaults to 16mb width.
	case Binary, Varbinary:
		return Binary
	case DateTime, TimestampNTZ:
		return TimestampNTZ
	default:
		return t
	}
}

func ToType(t string) (Type, error) {
	switch tt := Type(t); tt {
	case Number,
		Decimal,
		Numeric,
		Int,
		Integer,
		Bigint,
		Smallint,
		Tinyint,
		Byteint,
		Float,
		Float4,
		Float8,
		Double,
		DoublePrecision,
		Real,
		Varchar,
		Char,
		Character,
		String,
		Text,
		Binary,
		Varbinary,
		Boolean,
		Date,
		DateTime,
		Time,
		Timestamp,
		TimestampLTZ,
		TimestampNTZ,
		TimestampTZ,
		Variant,
		Object,
		Array,
		Geography,
		Geometry,
		Vector:
		return tt, nil
	}

	return "", errors.Errorf("invalid type %q", t)
}
