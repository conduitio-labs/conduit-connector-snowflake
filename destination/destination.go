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
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
	"github.com/snowflakedb/gosnowflake"
)

const (
	defaultBatchDelay = time.Second * 5
	defaultBatchSize  = 1000
)

type Destination struct {
	sdk.UnimplementedDestination

	config DestinationConfig
	Writer writer.Writer

	sdk.DestinationWithBatch
	connDSN string
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

// NewDestination creates the Destination and wraps it in the default middleware.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{
		DestinationWithBatch: sdk.DestinationWithBatch{
			BatchSize:  defaultBatchSize,
			BatchDelay: defaultBatchDelay,
		},
	})
}

// Open prepares the plugin to receive data from given position by
// initializing the database connection and creating the file stage if it does not exist.
func (d *Destination) Open(ctx context.Context) error {
	if err := d.snowflakeDSN(); err != nil {
		return errors.Errorf("failed to validate connection DSN: %w", err)
	}

	switch strings.ToUpper(d.config.Format) {
	case format.TypeCSV.String():
		w, err := writer.NewCSV(ctx, writer.SnowflakeConfig{
			Prefix:            d.config.NamingPrefix,
			PrimaryKey:        d.config.PrimaryKey,
			Stage:             d.config.Stage,
			Table:             d.config.Table,
			DSN:               d.connDSN,
			ProcessingWorkers: d.config.ProcessingWorkers,
			FileThreads:       d.config.FileUploadThreads,
			Compression:       d.config.Compression,
			CleanStageFiles:   d.config.AutoCleanupStage,
		})
		if err != nil {
			return errors.Errorf("csv writer: failed to open connection to snowflake: %w", err)
		}
		d.Writer = w
	default:
		return errors.Errorf("unknown format %q", d.config.Format)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
	// TODO: change to debug, using info for now to test with mdpx
	start := time.Now()

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

	sdk.Logger(ctx).Debug().Dur("duration", time.Since(start)).Msgf("finished processing records")

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

func (d *Destination) snowflakeDSN() error {
	utcTZ := "UTC"

	// The DSN requires an account, unfortunately this connector
	// does not have an explicit account config, thus it needs to be extracted from the host.
	parts := strings.Split(d.config.Host, ".")
	if parts[0] == "" {
		return errors.Errorf("unable to determine account from host %q", d.config.Host)
	}

	config := &gosnowflake.Config{
		Account:          parts[0],
		User:             d.config.Username,
		Password:         d.config.Password,
		Host:             d.config.Host,
		Port:             d.config.Port,
		Database:         d.config.Database,
		Schema:           d.config.Schema,
		Warehouse:        d.config.Warehouse,
		KeepSessionAlive: true,
		Params: map[string]*string{
			"TIMEZONE": &utcTZ,
		},
	}

	dsn, err := gosnowflake.DSN(config)
	if err != nil {
		return err
	}
	d.connDSN = dsn

	return nil
}
