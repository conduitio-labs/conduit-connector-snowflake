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

	"github.com/conduitio-labs/conduit-connector-snowflake/source/position"
	"github.com/conduitio/conduit-commons/csync"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"gopkg.in/tomb.v2"
)

// snapshotIterator to iterate snowflake objects.
type snapshotIterator struct {
	// repository for run queries to snowflake.
	snowflake Repository
	t       *tomb.Tomb
	workers []*FetchWorker
	acks    csync.WaitGroup

	conf snapshotIteratorConfig

	lastPosition position.Position

	data chan FetchData
}

type snapshotIteratorConfig struct {
	Position     position.Position
	// Tables to fetch rows from.
	Tables       []string
	// TableKeys: map of the column names per table that iterator use for setting key in record.
	TableKeys    map[string][]string
	// TableCols: map of columns per table for record payload
	// if empty - will get all columns.
	TableCols    	map[string][]string
	// TableOrderingCol: map of ordering column per table
	// if empty - will use the key instead.
	TableOrderingCol    	map[string]string
	// maxValue max value by ordering column per table when snapshot starts works
	maxValue map[string]any
	// size of snapshot batches to fetch
	batchSize int
}

func newSnapshotIterator(
	ctx context.Context,
	snowflake Repository,
	conf snapshotIteratorConfig,
	batchSize int,
	position *position.Position,
) (*snapshotIterator, error) {
	var err error

	t, _ := tomb.WithContext(ctx)
	it := &snapshotIterator{
		snowflake:      snowflake,
		t: t,
		conf: conf,
	}
	
	for _, table := range conf.Tables {
		p, ok := position.Snapshots[table]
		if position == nil || position.Snapshots == nil || !ok {
			p.SnapshotMaxValue, err = it.snowflake.GetMaxValue(ctx, table, conf.TableOrderingCol[table])
			if err != nil {
				return nil, errors.Errorf("get max value: %w", err)
			}
		}
	}

	if err := it.initFetchers(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize table fetchers: %w", err)
	}

	it.startWorkers()

	return it, nil
}

// Next get new record.
func (i *snapshotIterator) Next(_ context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, fmt.Errorf("iterator stopped: %w", ctx.Err())
	case d, ok := <-i.data:
		if !ok { // closed
			if err := i.t.Err(); err != nil {
				return sdk.Record{}, fmt.Errorf("fetchers exited unexpectedly: %w", err)
			}
			if err := i.acks.Wait(ctx); err != nil {
				return sdk.Record{}, fmt.Errorf("failed to wait for acks: %w", err)
			}
			return sdk.Record{}, ErrIteratorDone
		}

		i.acks.Add(1)
		return i.buildRecord(d), nil
	}
}

// Stop shutdown iterator.
func (i *snapshotIterator) Stop() error {
	if i.rows != nil {
		err := i.rows.Close()
		if err != nil {
			return errors.Errorf("close rows: %w", err)
		}
	}

	return i.snowflake.Close()
}

// Ack check if record with position was recorded.
func (i *snapshotIterator) Ack(ctx context.Context, rp sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(rp)).Msg("got ack")
	i.acks.Done()
	return nil
}

func (i *snapshotIterator) Teardown(_ context.Context) error {
	if i.t != nil {
		i.t.Kill(errors.New("tearing down snapshot iterator"))
	}

	return nil
}

func (i *snapshotIterator) initFetchers(ctx context.Context) error {
	var errs []error

	i.workers = make([]*FetchWorker, len(i.conf.Tables))

	for j, t := range i.conf.Tables {
		w := NewFetchWorker(i.snowflake, i.data, FetchConfig{
			Table:        t,
			Keys:          i.conf.TableKeys[t],
			Columns: i.conf.TableCols[t],
			FetchSize: i.conf.batchSize,
			Position:     i.lastPosition,
		})

		if err := w.Validate(ctx); err != nil {
			errs = append(errs, fmt.Errorf("failed to validate table fetcher %q config: %w", t, err))
		}

		i.workers[j] = w
	}

	return errors.Join(errs...)
}

func (i *snapshotIterator) startWorkers() {
	for j := range i.workers {
		f := i.workers[j]
		i.t.Go(func() error {
			ctx := i.t.Context(nil) //nolint:staticcheck // This is the correct usage of tomb.Context
			if err := f.Run(ctx); err != nil {
				return fmt.Errorf("fetcher for table %q exited: %w", f.conf.Table, err)
			}
			return nil
		})
	}
	go func() {
		<-i.t.Dead()
		close(i.data)
	}()
}