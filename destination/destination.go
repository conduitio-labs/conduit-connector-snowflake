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
	"github.com/conduitio/conduit-commons/config"
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

	Config Config
	Writer writer.Writer

	connDSN string
}

// NewDestination creates the Destination and wraps it in the default middleware.
func NewDestination() sdk.Destination {
	middlewares := sdk.DefaultDestinationMiddleware(sdk.DestinationWithBatchConfig{
		BatchSize:  defaultBatchSize,
		BatchDelay: defaultBatchDelay,
	})

	return sdk.DestinationWithMiddleware(&Destination{}, middlewares...)
}

func (d *Destination) Parameters() config.Parameters {
	return Config{}.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Debug().Msg("Configuring Destination Connector.")

	if err := sdk.Util.ParseConfig(ctx, cfg, &d.Config, NewDestination().Parameters()); err != nil {
		return errors.Errorf("failed to parse destination config: %w", err)
	}

	if err := d.snowflakeDSN(); err != nil {
		return errors.Errorf("failed to validate connection DSN: %w", err)
	}

	return nil
}

// Open prepares the plugin to receive data from given position by
// initializing the database connection and creating the file stage if it does not exist.
func (d *Destination) Open(ctx context.Context) error {
	switch strings.ToUpper(d.Config.Format) {
	case format.TypeCSV.String():
		w, err := writer.NewCSV(ctx, writer.SnowflakeConfig{
			Prefix:            d.Config.NamingPrefix,
			PrimaryKey:        d.Config.PrimaryKey,
			Stage:             d.Config.Stage,
			Table:             d.Config.Table,
			DSN:               d.connDSN,
			ProcessingWorkers: d.Config.ProcessingWorkers,
			FileThreads:       d.Config.FileUploadThreads,
			Compression:       d.Config.Compression,
			CleanStageFiles:   d.Config.AutoCleanupStage,
		})
		if err != nil {
			return errors.Errorf("csv writer: failed to open connection to snowflake: %w", err)
		}
		d.Writer = w
	default:
		return errors.Errorf("unknown format %q", d.Config.Format)
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
	parts := strings.Split(d.Config.Host, ".")
	if parts[0] == "" {
		return errors.Errorf("unable to determine account from host %q", d.Config.Host)
	}

	config := &gosnowflake.Config{
		Account:          parts[0],
		User:             d.Config.Username,
		Password:         d.Config.Password,
		Host:             d.Config.Host,
		Port:             d.Config.Port,
		Database:         d.Config.Database,
		Schema:           d.Config.Schema,
		Warehouse:        d.Config.Warehouse,
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
