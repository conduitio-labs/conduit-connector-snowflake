package source

import "github.com/conduitio-labs/conduit-connector-snowflake/config"

type SourceConfig struct {
	// shared config
	config.Config
	// Snapshot whether or not the plugin will take a snapshot of the entire table before starting cdc.
	Snapshot bool
	// BatchSize - size of batch.
	BatchSize int `validate:"gte=1,lte=100000"`
}
