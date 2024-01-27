package source

import "github.com/conduitio-labs/conduit-connector-snowflake/config"

//go:generate paramgen -output=config_paramgen.go Config

type Config struct {
	config.Config
	// Snapshot whether or not the plugin will take a snapshot of the entire table before starting cdc.
	Columns []string `json:"snowflake.columns" default:"false"`
	// Primary keys
	Keys []string `json:"snowflake.primaryKeys" `
	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"snowflake.orderingColumn" `
	// BatchSize - size of batch.
	BatchSize int `json:"snowflake.batchsize" default:"0"`
	// Snapshot
	Snapshot bool `json:"snowflake.snapshot" default:"false"`
}

const (
	KeyColumns = "snowflake.columns"
	// KeyPrimaryKeys is the list of the column names.
	KeyPrimaryKeys string = "snowflake.primaryKeys"
	// KeyOrderingColumn is a config name for an ordering column.
	KeyOrderingColumn = "snowflake.orderingColumn"
	// KeySnapshot is a config name for snapshotMode.
	KeySnapshot = "snowflake.snapshot"
	// KeyBatchSize is a config name for a batch size.
	KeyBatchSize = "snowflake.batchSize"
)
