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

package iterator

import (
	"context"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-snowflake/repository"
	"github.com/conduitio/conduit-connector-snowflake/source/position"
)

// Iterator combined iterator.
type Iterator struct {
	snowflake Repository

	snapshotIterator *SnapshotIterator
	cdcIterator      *CDCIterator

	pos sdk.Position

	table   string
	columns []string
	key     string
	limit   int
}

// New iterator.
func New(
	ctx context.Context,
	conn, table,
	key string,
	columns []string,
	limit int,
	pos sdk.Position,
) (*Iterator, error) {
	var (
		snapshotIterator *SnapshotIterator
		cdcIterator      *CDCIterator
		posType          position.IteratorType
	)

	snowflake, err := repository.Create(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("create snowflake repository: %v", err)
	}

	if pos == nil {
		posType = position.TypeSnapshot
	} else {
		posType, err = position.GetType(pos)
		if err != nil {
			return nil, fmt.Errorf("get position type: %v", err)
		}
	}

	switch posType {
	case position.TypeSnapshot:
		snapshotIterator, err = setupSnapshotIterator(ctx, snowflake, pos, table, key, columns, limit)
		if err != nil {
			return nil, fmt.Errorf("setup snapshot iterator: %v", err)
		}
	case position.TypeCDC:
		cdcIterator, err = setupCDCIterator(ctx, snowflake, pos, table, key, columns, limit, false)
		if err != nil {
			return nil, fmt.Errorf("setup cdc iterator: %v", err)
		}
	}

	return &Iterator{
		snowflake:        snowflake,
		snapshotIterator: snapshotIterator,
		cdcIterator:      cdcIterator,
		table:            table,
		columns:          columns,
		key:              key,
		pos:              pos,
		limit:            limit,
	}, nil
}

func setupSnapshotIterator(
	ctx context.Context,
	snowflake Repository,
	pos sdk.Position,
	table, key string,
	columns []string,
	limit int,
) (*SnapshotIterator, error) {
	var index int

	p, err := position.ParseSDKPosition(pos)
	if err != nil {
		return nil, fmt.Errorf("parse sdk position: %v", err)
	}

	if p.Element != 0 {
		index = p.Element + 1
	}

	data, err := snowflake.GetData(ctx, table, key, columns, p.Offset, limit)
	if err != nil {
		return nil, fmt.Errorf("get data: %v", err)
	}

	return NewSnapshotIterator(snowflake, table,
		columns, key, index, p.Offset, limit, data), nil
}

func setupCDCIterator(
	ctx context.Context,
	snowflake Repository,
	pos sdk.Position,
	table, key string,
	columns []string,
	limit int,
	needCreateStream bool,
) (*CDCIterator, error) {
	var index int

	p, err := position.ParseSDKPosition(pos)
	if err != nil {
		return nil, fmt.Errorf("parse sdk position: %v", err)
	}

	if p.Element != 0 {
		index = p.Element + 1
	}

	if needCreateStream {
		err = snowflake.CreateStream(ctx, getStreamName(table), table)
		if err != nil {
			return nil, fmt.Errorf("create stream: %v", err)
		}

		err = snowflake.CreateTrackingTable(ctx, getTrackingTable(table), table)
		if err != nil {
			return nil, fmt.Errorf("create stream: %v", err)
		}
	}

	data, err := snowflake.GetTrackingData(ctx, getStreamName(table), getTrackingTable(table), columns, p.Offset, limit)
	if err != nil {
		return nil, fmt.Errorf("get stream data: %v", err)
	}

	return NewCDCIterator(snowflake, table,
		columns, key, index, p.Offset, limit, data), nil
}

// HasNext check ability to get next record.
func (i *Iterator) HasNext(ctx context.Context) (bool, error) {
	if i.snapshotIterator != nil {
		hasNext, err := i.snapshotIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot iterator has next: %v", err)
		}

		if hasNext {
			return true, nil
		}

		// Setup cdc iterator.
		pos := position.NewPosition(position.TypeCDC, 0, 0)

		cdcIterator, err := setupCDCIterator(ctx, i.snapshotIterator.snowflake,
			pos.FormatSDKPosition(), i.table, i.key, i.columns, i.limit, true)
		if err != nil {
			return false, fmt.Errorf("setup cdc iterator: %v", err)
		}

		i.snapshotIterator = nil
		i.cdcIterator = cdcIterator

		hasNext, err = i.cdcIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("cdc iterator has next: %v", err)
		}

		return hasNext, nil
	}

	if i.cdcIterator != nil {
		hasNext, err := i.cdcIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("cdc iterator has next: %v", err)
		}

		return hasNext, nil
	}

	return false, ErrInvalidSetup
}

// Next get new record.
func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	if i.snapshotIterator != nil {
		return i.snapshotIterator.Next(ctx)
	}

	if i.cdcIterator != nil {
		return i.cdcIterator.Next(ctx)
	}

	return sdk.Record{}, ErrInvalidSetup
}

// Ack check if record with position was recorded.
func (i *Iterator) Ack(rp sdk.Position) error {
	if i.snapshotIterator != nil {
		return i.snapshotIterator.Ack(rp)
	}

	if i.cdcIterator != nil {
		return i.cdcIterator.Ack(rp)
	}

	return nil
}

// Stop iterator.
func (i *Iterator) Stop() error {
	if i.snapshotIterator != nil {
		return i.snapshotIterator.Stop()
	}

	if i.cdcIterator != nil {
		return i.cdcIterator.Stop()
	}

	return nil
}
