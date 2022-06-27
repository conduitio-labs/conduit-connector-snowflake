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
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// IteratorType describe position type.
type IteratorType string

const (
	TypeSnapshot = "s"
	TypeCDC      = "c"
)

// Position represents Snowflake position.
type Position struct {
	// IteratorType - shows in what iterator was created position.
	IteratorType IteratorType

	// IndexInBatch - index position in current batch.
	IndexInBatch int
	// BatchID - batch id.
	BatchID int
}

// NewPosition create position.
func NewPosition(iteratorType IteratorType, element int, batchID int) *Position {
	return &Position{IteratorType: iteratorType, IndexInBatch: element, BatchID: batchID}
}

// ParseSDKPosition parses SDK position and returns Position.
func ParseSDKPosition(p sdk.Position) (Position, error) {
	var pos Position

	if p == nil {
		return pos, nil
	}

	err := json.Unmarshal(p, &pos)
	if err != nil {
		return pos, err
	}

	switch pos.IteratorType {
	case TypeSnapshot:
		return pos, nil
	case TypeCDC:
		return pos, nil
	default:
		return pos, fmt.Errorf("%w : %s", ErrUnknownIteratorType, pos.IteratorType)
	}
}

// ConvertToSDKPosition formats and returns sdk.Position.
func (p Position) ConvertToSDKPosition() sdk.Position {
	b, _ := json.Marshal(p)

	return b
}
