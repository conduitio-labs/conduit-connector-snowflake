package destination

import (
	"context"
	"fmt"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/writer"
	"github.com/conduitio-labs/conduit-connector-snowflake/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	defaultBatchDelay = time.Second * 5
	defaultBatchSize  = 1000
)

type Destination struct {
	sdk.UnimplementedDestination
	repository *repository.Snowflake
	config     Config
	Writer     writer.Writer
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

	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("failed to parse destination config : %w", err)
	}

	return nil
}

// Open prepoares the plugin to receive data from given position by
// initializing the database connection and creating the file stage if it does not exist.
func (d *Destination) Open(ctx context.Context) error {

	writer, err := writer.NewSnowflake(ctx, &writer.SnowflakeConfig{
		Prefix:     d.config.NamingPrefix,
		PrimaryKey: d.config.PrimaryKey,
		Stage:      d.config.Stage,
		TableName:  d.config.Table,
		Connection: d.config.Connection,
		Format:     d.config.Format,
	})

	d.Writer = writer

	return err
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// TLDR - we don't need to implement custom batching logic, it's already handled
	// for us in the SDK, as long as we use sdk.batch.size / sdk.batch.delay.

	// sdk.batch.size and sdk.batch.delay already handles batching and should
	// control the size of records & timing of when Write() method is invoked.
	// FYI - these are only implemented in the SDK for destinations

	// General approach
	fmt.Println(" @@@@@@@@@ START WRITE - ")
	len, err := d.Writer.Write(ctx, records)

	return len, err
}

func (d *Destination) Teardown(ctx context.Context) error {
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	if d.repository != nil {
		d.repository.Close()
	}

	return nil
}
