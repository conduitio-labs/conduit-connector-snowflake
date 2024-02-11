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

package destination

import (
	"context"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
)

const (
	defaultBatchDelay = time.Second * 5
	defaultBatchSize  = 1000
)

type Destination struct {
	sdk.UnimplementedDestination

	Config Config
	Writer writer.Writer
}

// NewDestination creates the Destination and wraps it in the default middleware.
func NewDestination() sdk.Destination {
	// This is needed to override the default batch size and delay defaults for this destination connector.
	middlewares := sdk.DefaultDestinationMiddleware()
	for i, m := range middlewares {
		switch dest := m.(type) {
		case sdk.DestinationWithBatch:
			dest.DefaultBatchDelay = defaultBatchDelay
			dest.DefaultBatchSize = defaultBatchSize
			middlewares[i] = dest
		default:
		}
	}

	return sdk.DestinationWithMiddleware(&Destination{}, middlewares...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return Config{}.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	sdk.Logger(ctx).Debug().Msg("Configuring Destination Connector.")

	// TODO: add configuration for ordering column (aka primary key)
	// right now, we will automatically detect the key from the key within the record,
	// but it would be great to have flexibility in case the user wants to key on a different

	err := sdk.Util.ParseConfig(cfg, &d.Config)
	if err != nil {
		return errors.Errorf("failed to parse destination config: %w", err)
	}

	return nil
}

// Open prepares the plugin to receive data from given position by
// initializing the database connection and creating the file stage if it does not exist.
func (d *Destination) Open(ctx context.Context) error {
	switch strings.ToUpper(d.Config.Format) {
	case format.TypeCSV.String():
		w, err := writer.NewCSV(ctx, &writer.SnowflakeConfig{
			Prefix:        d.Config.NamingPrefix,
			PrimaryKey:    d.Config.PrimaryKey,
			Stage:         d.Config.Stage,
			TableName:     d.Config.Table,
			Connection:    d.Config.Connection,
			CSVGoroutines: d.Config.CSVGoroutines,
			FileThreads:   d.Config.FileUploadThreads,
		})
		if err != nil {
			return errors.Errorf("csv writer: failed to open connection to snowflake: %w", err)
		}
		d.Writer = w
	case format.TypeAVRO.String():
		aw, err := writer.NewAvro(ctx, &writer.SnowflakeConfig{
			Prefix:      d.Config.NamingPrefix,
			PrimaryKey:  d.Config.PrimaryKey,
			Stage:       d.Config.Stage,
			TableName:   d.Config.Table,
			Connection:  d.Config.Connection,
			FileThreads: d.Config.FileUploadThreads,
		})
		if err != nil {
			return errors.Errorf("avro writer: failed to open connector snowflake: %w", err)
		}

		d.Writer = aw
	default:
		return errors.Errorf("unknown format %q", d.Config.Format)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// TODO: change to debug, using info for now to test with mdpx
	sdk.Logger(ctx).Info().Msgf("batch contains %d records", len(records))

	// TLDR - we don't need to implement custom batching logic, it's already handled
	// for us in the SDK, as long as we use sdk.batch.size / sdk.batch.delay.

	// sdk.batch.size and sdk.batch.delay already handles batching and should
	// control the size of records & timing of when Write() method is invoked.
	// FYI - these are only implemented in the SDK for destinations

	n, err := d.Writer.Write(ctx, records)
	if err != nil {
		return 0, errors.Errorf("failed to write records: %w", err)
	}

	// TODO: change to debug, using info for now to test with mdpx
	sdk.Logger(ctx).Info().Msgf("wrote %d records", n)

	return n, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.Writer == nil {
		return nil
	}

	if err := d.Writer.Close(ctx); err != nil {
		return errors.Errorf("failed to gracefully close connection: %w", err)
	}

	return nil
}
