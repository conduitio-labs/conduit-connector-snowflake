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
	"maps"
	//"github.com/go-errors/errors"
)

type Schema map[string]Column

type Column struct {
	Name                     string
	Type                     Type
	Precision, Scale, Varlen int64
}

// ColumnEqual returns true when two columns are:
// * Strictly equal
// * Alias equal where precision and scale match
func ColumnEqual(c1, c2 Column) bool {
	if c1.Type == c2.Type || c1.Type.Unalias() == c2.Type.Unalias() {
		return true
	}

	t1 := c1.Type.Unalias()
	t2 := c2.Type.Unalias()

	switch {
	case t1 == Integer && t2 == Number:
		return c2.Precision == 38 && c2.Scale == 0 && c2.Varlen == -1
	case t1 == Number && t2 == Integer:
		return c1.Precision == 38 && c1.Scale == 0 && c1.Varlen == -1
	}

	return false
}

func (s Schema) Eq(s2 Schema) bool {
	return maps.EqualFunc(s, s2, ColumnEqual)
}

// implement Diff
// use schema to generate merges for update and delete.
// columns are always alphabetically ordered, with extra fields like meroxa ops last
// Upsert / Insert
