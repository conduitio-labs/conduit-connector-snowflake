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
	snapshotIterator *SnapshotIterator
	cdcIterator      *CDCIterator

	pos sdk.Position

	table   string
	columns []string
	key     string
}

// New iterator.
func New(
	ctx context.Context,
	conn, table,
	key string,
	columns []string,
	batchSize int,
	pos sdk.Position,
) (*Iterator, error) {
	var (
		snapshotIterator *SnapshotIterator
		cdcIterator      *CDCIterator
		posType          position.IteratorType
		isFirstStart     bool
		p                position.Position
		er               error
	)

	snowflake, err := repository.Create(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("create snowflake repository: %w", err)
	}

	// First start.
	if pos == nil {
		// Snapshot iterator starts work first.
		posType = position.TypeSnapshot

		isFirstStart = true

		// Prepare stream for cdc iterator.
		err = snowflake.CreateStream(ctx, getStreamName(table), table)
		if err != nil {
			return nil, fmt.Errorf("create stream: %w", err)
		}

		// Prepare tracking table for consume stream.
		err = snowflake.CreateTrackingTable(ctx, getTrackingTable(table), table)
		if err != nil {
			return nil, fmt.Errorf("create tracking table: %w", err)
		}
	} else {
		p, er = position.ParseSDKPosition(pos)
		if er != nil {
			return nil, fmt.Errorf("parse sdk position: %w", err)
		}

		posType = p.IteratorType
	}

	switch posType {
	case position.TypeSnapshot:
		snapshotIterator, err = setupSnapshotIterator(ctx, snowflake, table, key,
			columns, p.IndexInBatch, p.BatchID, batchSize, isFirstStart)
		if err != nil {
			return nil, fmt.Errorf("setup snapshot iterator: %w", err)
		}
	case position.TypeCDC:
		cdcIterator, err = setupCDCIterator(ctx, snowflake, table, key, columns, p.IndexInBatch, p.BatchID, batchSize)
		if err != nil {
			return nil, fmt.Errorf("setup cdc iterator: %w", err)
		}
	}

	return &Iterator{
		snapshotIterator: snapshotIterator,
		cdcIterator:      cdcIterator,
		table:            table,
		columns:          columns,
		key:              key,
		pos:              pos,
	}, nil
}

func setupSnapshotIterator(
	ctx context.Context,
	snowflake Repository,
	table, key string,
	columns []string,
	element, offset, batchSize int,
	isFirstStart bool,
) (*SnapshotIterator, error) {
	var (
		index int
	)

	if !isFirstStart {
		index = element + 1
	}

	data, err := snowflake.GetData(ctx, table, key, columns, offset, batchSize)
	if err != nil {
		return nil, fmt.Errorf("get currentBatch: %w", err)
	}

	return NewSnapshotIterator(snowflake, table,
		columns, key, index, offset, batchSize, data), nil
}

func setupCDCIterator(
	ctx context.Context,
	snowflake Repository,
	table, key string,
	columns []string,
	element, offset, batchSize int,
) (*CDCIterator, error) {
	var index int

	if element != 0 {
		index = element + 1
	}

	data, err := snowflake.GetTrackingData(ctx, getStreamName(table), getTrackingTable(table), columns,
		offset, batchSize)
	if err != nil {
		return nil, fmt.Errorf("get stream currentBatch: %w", err)
	}

	return NewCDCIterator(snowflake, table,
		columns, key, index, offset, batchSize, data), nil
}

// HasNext check ability to get next record.
func (i *Iterator) HasNext(ctx context.Context) (bool, error) {
	if i.snapshotIterator != nil {
		hasNext, err := i.snapshotIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("snapshot iterator has next: %w", err)
		}

		if hasNext {
			return true, nil
		}

		// Setup cdc iterator.
		cdcIterator, err := setupCDCIterator(ctx, i.snapshotIterator.snowflake,
			i.table, i.key, i.columns, 0, 0, i.snapshotIterator.batchSize)
		if err != nil {
			return false, fmt.Errorf("setup cdc iterator: %w", err)
		}

		i.cdcIterator = cdcIterator
		i.snapshotIterator = nil

		hasNext, err = i.cdcIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("cdc iterator has next: %w", err)
		}

		return hasNext, nil
	}

	if i.cdcIterator != nil {
		hasNext, err := i.cdcIterator.HasNext(ctx)
		if err != nil {
			return false, fmt.Errorf("cdc iterator has next: %w", err)
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
func (i *Iterator) Ack(ctx context.Context, rp sdk.Position) error {
	if i.snapshotIterator != nil {
		return i.snapshotIterator.Ack(ctx, rp)
	}

	if i.cdcIterator != nil {
		return i.cdcIterator.Ack(ctx, rp)
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
