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

package iterator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"
)

const defaultFetchSize = 50000

var supportedKeyTypes = []string{
	"smallint",
	"integer",
	"bigint",
}

type FetchConfig struct {
	Table        string
	Keys          []string
	Columns          []string
	OrderingColumn string
	FetchSize    int
	Position     position.Position
}

var (
	errTableRequired  = errors.New("table name is required")
	errKeysRequired    = errors.New("table keys required")
	errColsRequired    = errors.New("table columns required")
	errOrderColRequired    = errors.New("ordering column required")
	errInvalidCDCType = errors.New("invalid position type CDC")
)

func (c FetchConfig) Validate() error {
	var errs []error

	if c.Table == "" {
		errs = append(errs, errTableRequired)
	}

	if c.Keys == nil || len(c.Keys) == 0 {
		errs = append(errs, errKeysRequired)
	}

	if c.Columns == nil || len(c.Columns) == 0 {
		errs = append(errs, errColsRequired)
	}

	if c.OrderingColumn == "" {
		errs = append(errs, errOrderColRequired)
	}


	switch c.Position.IteratorType {
	case position.TypeSnapshot, position.TypeCDC:
	default:
		errs = append(errs, errInvalidCDCType)
	}

	return errors.Join(errs...)
}

type FetchData struct {
	Key      sdk.StructuredData
	Payload  sdk.StructuredData
	Position position.Position
	Table    string
}

type FetchWorker struct {
	conf FetchConfig
	snowflake Repository
	out  chan<- FetchData

	rows *sqlx.Rows
	snapshotLastProcessedVal any
	snapshotMaxValue    any
}

func NewFetchWorker(snowflake Repository, out chan<- FetchData, c FetchConfig) *FetchWorker {
	f := &FetchWorker{
		conf:       c,
		snowflake: snowflake,
		out:        out,
	}

	if f.conf.FetchSize == 0 {
		f.conf.FetchSize = defaultFetchSize
	}

	if c.Position.IteratorType == position.TypeSnapshot || c.Position.Snapshots == nil {
		return f
	}

	if p, ok := c.Position.Snapshots[c.Table]; ok {
		f.snapshotLastProcessedVal = p.SnapshotLastProcessedVal
		f.snapshotMaxValue = p.SnapshotMaxValue
	}

	return f
}

// Validate will ensure the config is correct.
// * Table and keys exist
// * Key is a primary key
func (f *FetchWorker) Validate(ctx context.Context) error {
	if err := f.conf.Validate(); err != nil {
		return fmt.Errorf("failed to validate config: %w", err)
	}


	tableExists, err := f.snowflake.TableExists(ctx, f.conf.Table)
	if err != nil {
		return fmt.Errorf("failed to validate table: %w", err)
	}
	if !tableExists {
		return fmt.Errorf("table %s does not exist", f.conf.Table)
	}

	columnsExist, missingColumns, err := f.snowflake.ColumnsExist(ctx, f.conf.Table, f.conf.Columns)
	if err != nil {
		return fmt.Errorf("failed to validate columns: %w", err)
	}
	if !columnsExist {
		sdk.Logger(ctx).Info().Msgf("table %s is missing columns %s", f.conf.Table, strings.Join(missingColumns, ","))
	}

	return nil
}


// HasNext check ability to get next record.
func (f *FetchWorker) HasNext(ctx context.Context) (bool, error) {
	var err error

	if f.rows != nil && f.rows.Next() {
		return true, nil
	}

	f.rows, err = f.snowflake.GetRows(ctx, f.conf.Table, f.conf.OrderingColumn, f.conf.Columns,
		&f.conf.Position, f.snapshotMaxValue, f.conf.FetchSize)
	if err != nil {
		// Snowflake library can return specific error for context cancel
		// Connector can't return this error and connector replace to
		// context cancel error
		if strings.Contains(err.Error(), snowflakeErrorCodeQueryNotExecuting) {
			return false, ctx.Err()
		}

		return false, fmt.Errorf("failed to get rows: %w", err)
	}

	// check new batch.
	if f.rows != nil && f.rows.Next() {
		return true, nil
	}

	return false, nil
}


// Next get new record.
func (f *FetchWorker) Next(_ context.Context) (sdk.Record, error) {
	row := make(map[string]any)
	if err := f.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	if _, ok := row[f.conf.OrderingColumn]; !ok {
		return sdk.Record{}, ErrOrderingColumnIsNotExist
	}

	snapPos := position.SnapshotPosition{
		SnapshotLastProcessedVal: row[f.conf.OrderingColumn],
		SnapshotMaxValue:         f.snapshotMaxValue,
	}

	pos := position.Position{
		IteratorType:             position.TypeSnapshot,
		Time:                     time.Now(),
	}

	sdkPos, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, errors.Errorf("convert position %w", err)
	}

	key := make(sdk.StructuredData)
	for n := range i.keys {
		val, ok := row[i.keys[n]]
		if !ok {
			return sdk.Record{}, errors.Errorf("key column %q not found", i.keys[n])
		}

		key[i.keys[n]] = val
	}

	transformedRowBytes, err := json.Marshal(row)
	if err != nil {
		return sdk.Record{}, errors.Errorf("marshal row: %w", err)
	}

	i.position = &pos

	metadata := sdk.Metadata(map[string]string{metadataTable: i.table})
	metadata.SetCreatedAt(time.Now())

	return sdk.Util.Source.NewRecordSnapshot(sdkPos, metadata, key, sdk.RawData(transformedRowBytes)), nil
}


func (f *FetchWorker) Run(ctx context.Context) error {
	// hasnext
	
	// next

	// pass record via channel
}

// func (f *FetchWorker) createCursor(ctx context.Context, tx pgx.Tx) (func(), error) {
// 	// This query will scan the table for rows based on the conditions.
// 	selectQuery := fmt.Sprintf(
// 		"SELECT * FROM %s WHERE %s > %d AND %s <= %d ORDER BY %s",
// 		f.conf.Table,
// 		f.conf.Key, f.lastRead, // range start
// 		f.conf.Key, f.snapshotEnd, // range end,
// 		f.conf.Key, // order by
// 	)

// 	cursorQuery := fmt.Sprintf("DECLARE %s CURSOR FOR(%s)", f.cursorName, selectQuery)

// 	if _, err := tx.Exec(ctx, cursorQuery); err != nil {
// 		return nil, err
// 	}

// 	return func() {
// 		// N.B. The cursor will automatically close when the TX is done.
// 		if _, err := tx.Exec(ctx, "CLOSE "+f.cursorName); err != nil {
// 			sdk.Logger(ctx).Warn().
// 				Err(err).
// 				Msgf("unexpected error when closing cursor %q", f.cursorName)
// 		}
// 	}, nil
// }

// func (f *FetchWorker) updateSnapshotEnd(ctx context.Context, tx pgx.Tx) error {
// 	if f.snapshotEnd > 0 {
// 		return nil
// 	}

// 	if err := tx.QueryRow(
// 		ctx,
// 		fmt.Sprintf("SELECT max(%s) FROM %s", f.conf.Key, f.conf.Table),
// 	).Scan(&f.snapshotEnd); err != nil {
// 		return fmt.Errorf("failed to query max on %q.%q: %w", f.conf.Table, f.conf.Key, err)
// 	}

// 	return nil
// }

// func (f *FetchWorker) fetch(ctx context.Context, tx pgx.Tx) (int, error) {
// 	rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM %s", f.conf.FetchSize, f.cursorName))
// 	if err != nil {
// 		return 0, fmt.Errorf("failed to fetch rows: %w", err)
// 	}
// 	defer rows.Close()

// 	var fields []string
// 	for _, f := range rows.FieldDescriptions() {
// 		fields = append(fields, f.Name)
// 	}

// 	var nread int

// 	for rows.Next() {
// 		values, err := rows.Values()
// 		if err != nil {
// 			return 0, fmt.Errorf("failed to get values: %w", err)
// 		}

// 		data, err := f.buildFetchData(fields, values)
// 		if err != nil {
// 			return nread, fmt.Errorf("failed to build fetch data: %w", err)
// 		}

// 		if err := f.send(ctx, data); err != nil {
// 			return nread, fmt.Errorf("failed to send record: %w", err)
// 		}

// 		nread++
// 	}
// 	if rows.Err() != nil {
// 		return 0, fmt.Errorf("failed to read rows: %w", rows.Err())
// 	}

// 	return nread, nil
// }

func (f *FetchWorker) send(ctx context.Context, d FetchData) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case f.out <- d:
		return nil
	}
}


