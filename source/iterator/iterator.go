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
	"strings"

	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
	"github.com/conduitio/conduit-commons/opencdc"
)

// Iterator combined iterator.
type Iterator struct {
	snapshotIterator *snapshotIterator
	cdcIterator      *CDCIterator

	pos opencdc.Position

	table   string
	columns []string
	keys    []string
}

// New iterator.
func New(
	ctx context.Context,
	conn, table,
	orderingColumn string,
	keys, columns []string,
	batchSize int,
	snapshot bool,
	pos opencdc.Position,
) (*Iterator, error) {
	it := &Iterator{
		table:   table,
		columns: columns,
		keys:    keys,
		pos:     pos,
	}

	snowflake, err := repository.Connect(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("create snowflake repository: %w", err)
	}

	// prepare CDC for the first connector run.
	if pos == nil {
		err = prepareCDC(ctx, snowflake, table)
		if err != nil {
			return nil, fmt.Errorf("prepare cdc: %w", err)
		}
	}

	p, err := position.ParseSDKPosition(pos)
	if err != nil {
		return nil, fmt.Errorf("parse sdk position: %w", err)
	}

	err = it.populateKeyColumns(ctx, snowflake, orderingColumn)
	if err != nil {
		return nil, fmt.Errorf("populate keys: %w", err)
	}

	if snapshot && (p == nil || p.IteratorType == position.TypeSnapshot) {
		it.snapshotIterator, err = newSnapshotIterator(ctx, snowflake, table, orderingColumn, it.keys, columns, batchSize, p)
		if err != nil {
			return nil, fmt.Errorf("setup snapshot iterator: %w", err)
		}
	} else {
		it.cdcIterator, err = setupCDCIterator(ctx, snowflake, table, it.keys, columns, p, batchSize)
		if err != nil {
			return nil, fmt.Errorf("setup cdc iterator: %w", err)
		}
	}

	return it, nil
}

func prepareCDC(ctx context.Context, snowflake *repository.Snowflake, table string) error {
	// Check if the tracking table exists.
	isTableExist, err := snowflake.TableExists(ctx, getTrackingTableName(table))
	if err != nil {
		return fmt.Errorf("check if table exist: %w", err)
	}

	if !isTableExist {
		// Prepare tracking table for consume stream.
		err = snowflake.CreateTrackingTable(ctx, getTrackingTableName(table), table)
		if err != nil {
			return fmt.Errorf("create tracking table: %w", err)
		}
	}

	// Prepare stream for cdc iterator.
	err = snowflake.CreateStream(ctx, getStreamName(table), table)
	if err != nil {
		return fmt.Errorf("create stream: %w", err)
	}

	return nil
}

func setupCDCIterator(
	ctx context.Context,
	snowflake Repository,
	table string,
	keys, columns []string,
	p *position.Position,
	batchSize int,
) (*CDCIterator, error) {
	var index, offset int
	if p != nil {
		offset = p.BatchID
	}

	if p != nil {
		index = p.IndexInBatch + 1
	}

	data, err := snowflake.GetTrackingData(ctx, getStreamName(table), getTrackingTableName(table), columns,
		offset, batchSize)
	if err != nil {
		// Snowflake library sends request to abort query with query and to server when get context cancel.
		// But sometimes query was executed or didn't start execution.
		// On this case snowflake server return specific error:
		// 000605: Identified SQL statement is not currently executing.
		// Connector can't return this error and connector replace to
		// context cancel error
		// https://github.com/snowflakedb/gosnowflake/blob/master/restful.go#L449
		if strings.Contains(err.Error(), snowflakeErrorCodeQueryNotExecuting) {
			return nil, ctx.Err()
		}

		return nil, fmt.Errorf("get stream currentBatch: %w", err)
	}

	return NewCDCIterator(snowflake, table, keys, columns, index, offset, batchSize, data), nil
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
			i.table, i.keys, i.columns, nil, i.snapshotIterator.batchSize)
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
func (i *Iterator) Next(ctx context.Context) (opencdc.Record, error) {
	if i.snapshotIterator != nil {
		return i.snapshotIterator.Next(ctx)
	}

	if i.cdcIterator != nil {
		return i.cdcIterator.Next(ctx)
	}

	return opencdc.Record{}, ErrInvalidSetup
}

// Ack check if record with position was recorded.
func (i *Iterator) Ack(ctx context.Context, rp opencdc.Position) error {
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

// populates keyColumn from the database's metadata
// or from the orderingColumn configuration field.
func (i *Iterator) populateKeyColumns(
	ctx context.Context,
	snowflake Repository,
	orderingColumn string,
) error {
	if len(i.keys) != 0 {
		return nil
	}

	var err error

	i.keys, err = snowflake.GetPrimaryKeys(ctx, i.table)
	if err != nil {
		return fmt.Errorf("get primary keys: %w", err)
	}

	if len(i.keys) != 0 {
		return nil
	}

	// use orderingColumn in case primary keys don't exist
	i.keys = []string{orderingColumn}

	return nil
}
