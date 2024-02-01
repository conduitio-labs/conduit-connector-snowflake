package destination

import (
	"context"
	"slices"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/schema"
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
	Schema schema.Schema
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
	writer, err := writer.NewSnowflake(ctx, &writer.SnowflakeConfig{
		Prefix:     d.Config.NamingPrefix,
		PrimaryKey: d.Config.PrimaryKey,
		Stage:      d.Config.Stage,
		TableName:  d.Config.Table,
		Connection: d.Config.Connection,
		Format:     d.Config.Format,
	})
	if err != nil {
		return errors.Errorf("failed to open connection to snowflake: %w", err)
	}

	d.Writer = writer

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	if d.Schema == nil {
		if err := d.initSchema(records); err != nil {
			return 0, errors.Errorf("failed to initialize schema from records: %w", err)
		}
	}

	// TLDR - we don't need to implement custom batching logic, it's already handled
	// for us in the SDK, as long as we use sdk.batch.size / sdk.batch.delay.

	// sdk.batch.size and sdk.batch.delay already handles batching and should
	// control the size of records & timing of when Write() method is invoked.
	// FYI - these are only implemented in the SDK for destinations

	len, err := d.Writer.Write(ctx, records)
	if err != nil {
		return 0, errors.Errorf("failed to write records: %w", err)
	}

	return len, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.Writer == nil {
		return nil
	}

	if err := d.Writer.Close(); err != nil {
		return errors.Errorf("failed to gracefully close connection: %w", err)
	}

	return nil
}

func (d *Destination) initSchema(records []sdk.Record) error {
	i := slices.IndexFunc(records, func(r sdk.Record) bool {
		return r.Operation == sdk.OperationSnapshot || r.Operation == sdk.OperationCreate
	})

	if i < 0 {
		return errors.New("failed to find suitable record to infer schema")
	}

	s, err := schema.Parse(records[i])
	if err != nil {
		return errors.Errorf("failed to infer schema from record: %w", err)
	}

	d.Schema = s

	return nil
}
