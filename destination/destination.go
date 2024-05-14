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
	"bytes"
	"context"
	"fmt"
	"html/template"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/sprig"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/go-errors/errors"
)

const (
	defaultBatchDelay = time.Second * 5
	defaultBatchSize  = 1000
	keepAliveParam    = "CLIENT_SESSION_KEEP_ALIVE"
)

type Destination struct {
	sdk.UnimplementedDestination

	Config Config
	Writer writer.Writer
	getTableName TableFn
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
		connectionString := d.configureURL()
		w, err := writer.NewCSV(ctx, &writer.SnowflakeConfig{
			Prefix:            d.Config.NamingPrefix,
			PrimaryKey:        d.Config.PrimaryKey,
			Stage:             d.Config.Stage,
			TableName:         d.Config.Table,
			Connection:        connectionString,
			ProcessingWorkers: d.Config.ProcessingWorkers,
			FileThreads:       d.Config.FileUploadThreads,
			Compression:       d.Config.Compression,
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

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// TODO: change to debug, using info for now to test with mdpx
	start := time.Now()

	sdk.Logger(ctx).Info().Msgf("batch contains %d records", len(records))

	// TLDR - we don't need to implement custom batching logic, it's already handled
	// for us in the SDK, as long as we use sdk.batch.size / sdk.batch.delay.

	// sdk.batch.size and sdk.batch.delay already handles batching and should
	// control the size of records & timing of when Write() method is invoked.
	// FYI - these are only implemented in the SDK for destinations

	// check if multiple collections is being used
	// TODO: revisit this to use smarter preallocation for large batches
	recordsPerTable := map[string][]sdk.Record{}
	for _, r := range records {
		table, err := d.getTableName(r)
		if err != nil {
			return 0, fmt.Errorf("invalid table name or table function: %w", err)
		}
		if recs, ok := recordsPerTable[table]; ok {
			// TODO: revisit this to use smarter preallocation for large batches. this approach is just to prove the concept.
			recs = append(recs, r)
			recordsPerTable[table] = recs
		} else {
			recordsPerTable[table] = []sdk.Record{r}
		}
	}

	// if so, split the record batches into multiple batches by destination table

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

func (d *Destination) configureURL() string {
	paramMap := map[string]string{
		"warehouse":    d.Config.Warehouse,
		keepAliveParam: strconv.FormatBool(d.Config.KeepAlive),
	}

	paramStrings := make([]string, len(paramMap))
	i := 0
	for k, v := range paramMap {
		paramStrings[i] = fmt.Sprintf("%s=%s", k, v)
		i++
	}

	paramStr := strings.Join(paramStrings, "&")

	return fmt.Sprintf("%s:%s@%s:%d/%s/%s?%s",
		d.Config.Username,
		d.Config.Password,
		d.Config.Host,
		d.Config.Port,
		d.Config.Database,
		d.Config.Schema,
		paramStr,
	)
}

// TableFunction returns a function that determines the table for each record individually.
// The function might be returning a static table name.
// If the table is neither static nor a template, an error is returned.
func (c Config) TableFunction() (f TableFn, err error) {
	// Not a template, i.e. it's a static table name
	if !strings.HasPrefix(c.Table, "{{") && !strings.HasSuffix(c.Table, "}}") {
		return func(_ sdk.Record) (string, error) {
			return c.Table, nil
		}, nil
	}

	// Try to parse the table
	t, err := template.New("table").Funcs(sprig.FuncMap()).Parse(c.Table)
	if err != nil {
		// The table is not a valid Go template.
		return nil, fmt.Errorf("table is neither a valid static table nor a valid Go template: %w", err)
	}

	// The table is a valid template, return TableFn.
	var buf bytes.Buffer
	return func(r sdk.Record) (string, error) {
		buf.Reset()
		if err := t.Execute(&buf, r); err != nil {
			return "", fmt.Errorf("failed to execute table template: %w", err)
		}
		return buf.String(), nil
	}, nil
}
