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

package position

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// IteratorType describe position type.
type IteratorType string

const (
	indexType = iota
	indexElement
	indexOffset
	indexOffsetTotal

	TypeSnapshot = "s"
	TypeCDC      = "c"

	posFormat = "%s.%d.%d.%d"
)

// Position represents Snowflake position.
type Position struct {
	IteratorType IteratorType

	Element       int
	Offset        int
	SnapshotTotal int
}

// NewPosition create position.
func NewPosition(iteratorType IteratorType, element int, offset int, total int) *Position {
	return &Position{IteratorType: iteratorType, Element: element, Offset: offset, SnapshotTotal: total}
}

// ParseSDKPosition parses SDK position and returns Position.
func ParseSDKPosition(p sdk.Position) (Position, error) {
	var iteratorType IteratorType

	if p == nil {
		return Position{}, nil
	}

	parts := strings.Split(string(p), ".")

	if len(parts) != reflect.TypeOf(Position{}).NumField() {
		return Position{}, fmt.Errorf("the number of position elements must be equal to %d, now it is %d",
			reflect.TypeOf(Position{}).NumField(), len(parts))
	}

	switch parts[indexType] {
	case TypeSnapshot:
		iteratorType = TypeSnapshot
	case TypeCDC:
		iteratorType = TypeCDC
	default:
		return Position{}, ErrInvalidType
	}

	element, err := strconv.Atoi(parts[indexElement])
	if err != nil {
		return Position{}, ErrFieldInvalidElement
	}

	offset, err := strconv.Atoi(parts[indexOffset])
	if err != nil {
		return Position{}, ErrFieldInvalidOffset
	}

	total, err := strconv.Atoi(parts[indexOffsetTotal])
	if err != nil {
		return Position{}, ErrFieldInvalidOffset
	}

	return Position{
		IteratorType:  iteratorType,
		Element:       element,
		Offset:        offset,
		SnapshotTotal: total,
	}, nil
}

// FormatSDKPosition formats and returns sdk.Position.
func (p Position) FormatSDKPosition() sdk.Position {
	return sdk.Position(fmt.Sprintf(posFormat, p.IteratorType, p.Element, p.Offset, p.SnapshotTotal))
}

// GetType get position type.
func GetType(p sdk.Position) (IteratorType, error) {
	parts := strings.Split(string(p), ".")

	if parts[0] == TypeSnapshot {
		return TypeSnapshot, nil
	}

	if parts[0] == TypeCDC {
		return TypeCDC, nil
	}

	return "", ErrInvalidType
}
