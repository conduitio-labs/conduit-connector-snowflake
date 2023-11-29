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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
)

// snapshotIterator to iterate snowflake objects.
type snapshotIterator struct {
	rows *sqlx.Rows

	// repository for run queries to snowflake.
	snowflake Repository

	// table - table in snowflake for getting currentBatch.
	table string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// keys is the list of the column names that iterator use for setting key in record.
	keys []string
	// batchSize size of batch.
	batchSize int
	// orderingColumn Name of column what iterator using for sorting data.
	orderingColumn string
	// position last recorded position.
	position *position.Position
	// maxValue max value by ordering column when snapshot starts works
	maxValue any
}

func newSnapshotIterator(
	ctx context.Context,
	snowflake Repository,
	table, orderingColumn string,
	keys, columns []string,
	batchSize int,
	position *position.Position,
) (*snapshotIterator, error) {
	var (
		err error
	)

	iterator := &snapshotIterator{
		snowflake:      snowflake,
		table:          table,
		columns:        columns,
		keys:           keys,
		orderingColumn: orderingColumn,
		batchSize:      batchSize,
		position:       position,
	}

	if position == nil {
		iterator.maxValue, err = iterator.snowflake.GetMaxValue(ctx, table, orderingColumn)
		if err != nil {
			return nil, fmt.Errorf("get max value: %w", err)
		}
	} else {
		iterator.maxValue = position.SnapshotMaxValue
	}

	return iterator, nil
}

// HasNext check ability to get next record.
func (i *snapshotIterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	i.rows, err = i.snowflake.GetRows(ctx, i.table, i.orderingColumn, i.columns,
		i.position, i.maxValue, i.batchSize)
	if err != nil {
		// Snowflake library can return specific error for context cancel
		// Connector can't return this error and connector replace to
		// context cancel error
		if strings.Contains(err.Error(), snowflakeErrorCodeQueryNotExecuting) {
			return false, ctx.Err()
		}

		return false, fmt.Errorf("get rows: %w", err)
	}

	// check new batch.
	if i.rows != nil && i.rows.Next() {
		return true, nil
	}

	return false, nil
}

// Next get new record.
func (i *snapshotIterator) Next(_ context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := i.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	if _, ok := row[i.orderingColumn]; !ok {
		return sdk.Record{}, ErrOrderingColumnIsNotExist
	}

	pos := position.Position{
		IteratorType:             position.TypeSnapshot,
		SnapshotLastProcessedVal: row[i.orderingColumn],
		SnapshotMaxValue:         i.maxValue,
		Time:                     time.Now(),
	}

	sdkPos, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert position %w", err)
	}

	key := make(sdk.StructuredData)
	for n := range i.keys {
		val, ok := row[i.keys[n]]
		if !ok {
			return sdk.Record{}, fmt.Errorf("key column %q not found", i.keys[n])
		}

		key[i.keys[n]] = val
	}

	transformedRowBytes, err := json.Marshal(row)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal row: %w", err)
	}

	i.position = &pos

	metadata := sdk.Metadata(map[string]string{metadataTable: i.table})
	metadata.SetCreatedAt(time.Now())

	return sdk.Util.Source.NewRecordSnapshot(sdkPos, metadata, key, sdk.RawData(transformedRowBytes)), nil
}

// Stop shutdown iterator.
func (i *snapshotIterator) Stop() error {
	if i.rows != nil {
		err := i.rows.Close()
		if err != nil {
			return fmt.Errorf("close rows: %w", err)
		}
	}

	return i.snowflake.Close()
}

// Ack check if record with position was recorded.
func (i *snapshotIterator) Ack(ctx context.Context, rp sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(rp)).Msg("got ack")

	return nil
}
