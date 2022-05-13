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

package snapshot

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-snowflake/source/position"
)

// Position represents Snowflake position.
type Position struct {
	Type position.Type

	Element int
	Offset  int
}

// NewPosition create position.
func NewPosition(element int, offset int) *Position {
	return &Position{Type: position.TypeSnapshot, Element: element, Offset: offset}
}

// ParseSDKPosition parses SDK position and returns Position.
func ParseSDKPosition(p sdk.Position) (Position, error) {
	if p == nil {
		return Position{}, nil
	}

	parts := strings.Split(string(p), ".")

	if len(parts) != reflect.TypeOf(Position{}).NumField() {
		return Position{}, fmt.Errorf("the number of position elements must be equal to %d, now it is %d",
			reflect.TypeOf(Position{}).NumField(), len(parts))
	}

	if parts[0] != position.TypeSnapshot {
		return Position{}, position.ErrInvalidType
	}

	element, err := strconv.Atoi(parts[1])
	if err != nil {
		return Position{}, position.ErrFieldInvalidElement
	}

	offset, err := strconv.Atoi(parts[2])
	if err != nil {
		return Position{}, position.ErrFieldInvalidOffset
	}

	return Position{
		Type:    position.TypeSnapshot,
		Element: element,
		Offset:  offset,
	}, nil
}

// FormatSDKPosition formats and returns sdk.Position.
func (p Position) FormatSDKPosition() sdk.Position {
	return sdk.Position(fmt.Sprintf("%s.%d.%d", p.Type, p.Element, p.Offset))
}
