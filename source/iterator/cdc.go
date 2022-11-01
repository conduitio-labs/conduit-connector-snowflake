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
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jmoiron/sqlx"

	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
)

// CDCIterator to iterate snowflake objects.
type CDCIterator struct {
	rows *sqlx.Rows
	mu   sync.RWMutex

	// repository for run queries to snowflake.
	snowflake Repository
	// table - table in snowflake for getting currentBatch.
	table string
	// trackingTable - trackingTable name.
	trackingTable string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// Name of column what iterator use for setting key in record.
	key string
	// batchSize size of batch.
	batchSize int
	// processedIds - counter of processed ids.
	processedIds int
	// removedIds - counter of removed ids.
	removedIds int
	// rowIDS - ids for removing.
	trackingIDsCh chan string
	// stopCh for graceful shutdown.
	stopCh chan struct{}
	// finishClearCh for check if clearing was finished.
	finishClearCh chan struct{}
	// errCh for errors.
	errCh chan error
}

func NewCDCIterator(
	snowflake Repository,
	table string,
	columns []string,
	key string,
	batchSize int,
) *CDCIterator {
	return &CDCIterator{
		snowflake:     snowflake,
		table:         table,
		trackingTable: getTrackingTable(table),
		columns:       columns,
		key:           key,
		batchSize:     batchSize,
		trackingIDsCh: make(chan string, 100),
		stopCh:        make(chan struct{}, 1),
		finishClearCh: make(chan struct{}, 1),
		errCh:         make(chan error, 10),
	}
}

// HasNext check ability to get next record.
func (c *CDCIterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	if c.rows != nil && c.rows.Next() {
		val, er := c.isUpdateInfoFirstRow()
		if er != nil {
			return false, fmt.Errorf("is update first row: %w", er)
		}

		if val {
			return c.HasNext(ctx)
		}

		return true, nil
	}

	for {
		if c.processedIds == c.removedIds {
			break
		}

		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		time.Sleep(2 * time.Second)
	}

	c.processedIds = 0

	c.mu.Lock()
	c.removedIds = 0
	c.mu.Unlock()

	c.rows, err = c.snowflake.GetTrackingData(ctx, getStreamName(c.table),
		c.trackingTable, c.columns, c.batchSize)
	if err != nil {
		if strings.Contains(err.Error(), snowflakeErrorCodeQueryNotExecuting) {
			return false, ctx.Err()
		}

		return false, err
	}

	// check new batch.
	if c.rows != nil && c.rows.Next() {
		val, er := c.isUpdateInfoFirstRow()
		if er != nil {
			return false, fmt.Errorf("is update first row: %w", er)
		}

		if val {
			return c.HasNext(ctx)
		}

		return true, nil
	}

	return false, nil
}

// Next get new record.
func (c *CDCIterator) Next(ctx context.Context) (sdk.Record, error) {
	var (
		payload sdk.RawData
		err     error
		record  sdk.Record
	)

	row := make(map[string]any)
	if err = c.rows.MapScan(row); err != nil {
		return sdk.Record{}, fmt.Errorf("scan rows: %w", err)
	}

	rowID, ok := row[repository.MetadataColumnRow].(string)
	if !ok {
		return sdk.Record{}, ErrNotStringType
	}

	pos := position.Position{
		IteratorType: position.TypeCDC,
		RowID:        rowID,
		Time:         time.Now(),
	}

	action, err := getAction(row)
	if err != nil {
		return record, fmt.Errorf("get action: %w", err)
	}

	// remove metadata columns.
	delete(row, repository.MetadataColumnUpdate)
	delete(row, repository.MetadataColumnAction)
	delete(row, repository.MetadataColumnRow)
	delete(row, repository.MetadataColumnTime)

	payload, err = json.Marshal(row)
	if err != nil {
		return record, fmt.Errorf("marshal error : %w", err)
	}

	if _, ok = row[c.key]; !ok {
		return record, ErrKeyIsNotExist
	}

	key := row[c.key]

	metadata := sdk.Metadata(map[string]string{metadataTable: c.table})
	metadata.SetCreatedAt(time.Now())

	p, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("convert to sdk position:%w", err)
	}

	c.processedIds++

	switch action {
	case actionInsert:
		return sdk.Util.Source.NewRecordCreate(p, metadata,
			sdk.StructuredData{c.key: key}, payload), nil
	case actionUpdate:
		return sdk.Util.Source.NewRecordUpdate(p, metadata,
			sdk.StructuredData{c.key: key}, nil, payload), nil
	case actionDelete:
		return sdk.Util.Source.NewRecordDelete(p, metadata,
			sdk.StructuredData{c.key: key}), nil
	default:
		return record, ErrCantFindActionType
	}
}

// Stop shutdown iterator.
func (c *CDCIterator) Stop(ctx context.Context) error {
	if c.rows != nil {
		err := c.rows.Close()
		if err != nil {
			return fmt.Errorf("close rows: %w", err)
		}
	}

	if c.rows != nil {
		err := c.rows.Close()
		if err != nil {
			return err
		}
	}

	c.stopCh <- struct{}{}

	select {
	case <-c.finishClearCh:
		sdk.Logger(ctx).Debug().Msg("stopped clearing table")
	case <-time.After(30 * time.Second):
		sdk.Logger(ctx).Warn().Msg("worked timeout")
	}

	// close channels
	close(c.trackingIDsCh)
	close(c.stopCh)
	close(c.finishClearCh)
	close(c.errCh)

	return c.snowflake.Close()
}

// Ack check if record with position was recorded.
func (c *CDCIterator) Ack(ctx context.Context, rp sdk.Position) error {
	pos, err := position.ParseSDKPosition(rp)
	if err != nil {
		return err
	}

	c.trackingIDsCh <- pos.RowID

	return nil
}

// isUpdateInfoFirstRow method check if current row is first stream row info about update operation.
// Snowflake stream save two rows about update info:
// 1) where metadata actionType = delete and metadata update = true, this is deleting.
// 2) where metadata actionType = insertValue and update = true, this is exactly updating.
// Skip first part and work only with second to avoid mistakes.
func (c *CDCIterator) isUpdateInfoFirstRow() (bool, error) {
	row := make(map[string]any)
	if err := c.rows.MapScan(row); err != nil {
		return false, fmt.Errorf("scan rows: %w", err)
	}

	if row[repository.MetadataColumnAction] == deleteValue && row[repository.MetadataColumnUpdate] == true {
		val, ok := row[repository.MetadataColumnRow].(string)
		if !ok {
			return false, ErrNotStringType
		}

		c.trackingIDsCh <- val

		c.processedIds++

		return true, nil
	}

	return false, nil
}

// ClearTrackingTable remove recorded rows from tracking table.
func (c *CDCIterator) ClearTrackingTable(ctx context.Context) {
	var ids []any

	for {
		select {
		case id := <-c.trackingIDsCh:
			c.mu.Lock()
			c.removedIds++
			c.mu.Unlock()

			if ids == nil {
				ids = make([]any, 0)
			}

			ids = append(ids, id)
			if len(ids) >= idsClearingBufferSize {
				err := c.snowflake.DeleteTrackingData(ctx, c.trackingTable, ids)
				if err != nil {
					c.errCh <- fmt.Errorf("delete rows: %w", err)
				}

				ids = nil
			}

		case <-time.After(clearingDuration * time.Second):
			err := c.snowflake.DeleteTrackingData(ctx, c.trackingTable, ids)
			if err != nil {
				c.errCh <- fmt.Errorf("delete rows: %w", err)

				return
			}

			ids = nil

		case <-c.stopCh:
			err := c.snowflake.DeleteTrackingData(ctx, c.trackingTable, ids)
			if err != nil {
				c.errCh <- fmt.Errorf("delete rows: %w", err)
			}

			c.finishClearCh <- struct{}{}

			return
		}
	}
}

func getStreamName(table string) string {
	return fmt.Sprintf(nameFormat, Conduit, "stream", table)
}

func getTrackingTable(table string) string {
	return fmt.Sprintf(nameFormat, Conduit, "tracking", table)
}

func getAction(data map[string]interface{}) (actionType, error) {
	if data[repository.MetadataColumnAction] == insertValue && data[repository.MetadataColumnUpdate] == false {
		return actionInsert, nil
	}

	if data[repository.MetadataColumnAction] == insertValue && data[repository.MetadataColumnUpdate] == true {
		return actionUpdate, nil
	}

	if data[repository.MetadataColumnAction] == deleteValue && data[repository.MetadataColumnUpdate] == false {
		return actionDelete, nil
	}

	return "", ErrCantFindActionType
}
