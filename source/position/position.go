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
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/go-errors/errors"
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

	// Snapshot information.
	// SnapshotLastProcessedVal - last processed value from ordering column.
	SnapshotLastProcessedVal any
	// SnapshotMaxValue max value by  ordering column, when snapshot starts work.
	SnapshotMaxValue any

	// CDC information
	// IndexInBatch - index position in current batch.
	IndexInBatch int
	// BatchID - batch id.
	BatchID int

	// Time Created time.
	Time time.Time
}

// ParseSDKPosition parses SDK position and returns Position.
func ParseSDKPosition(p opencdc.Position) (*Position, error) {
	var pos Position

	if p == nil {
		return nil, nil
	}

	err := json.Unmarshal(p, &pos)
	if err != nil {
		return nil, err
	}

	switch pos.IteratorType {
	case TypeSnapshot:
		return &pos, nil
	case TypeCDC:
		return &pos, nil
	default:
		return &pos, errors.Errorf("%w : %s", ErrUnknownIteratorType, pos.IteratorType)
	}
}

// ConvertToSDKPosition formats and returns opencdc.Position.
func (p Position) ConvertToSDKPosition() (opencdc.Position, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return opencdc.Position{}, err
	}

	return b, nil
}
