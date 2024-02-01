package destination

import (
	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/format"
)

//go:generate paramgen -output=config_paramgen.go Config

type Config struct {
	config.Config
	// Snowflake Stage data is copied into before merge
	Stage string `json:"snowflake.stage" default:"0"`
	// Primary key(s) of the source table
	PrimaryKey []string `json:"snowflake.primaryKey"`
	// Prefix to append to update_at , deleted_at, create_at at destination table
	NamingPrefix string `json:"snowflake.namingPrefix" default:"meroxa"`
	// Data type of file we upload and copy data from to snowflake
	Format format.Format `json:"snowflake.format" validate:"required,inclusion=csv"`
}

const (
	SnowflakeStage        = "snowflake.stage"
	SnowflakePrimaryKey   = "snowflake.primaryKey"
	SnowflakeNamingPrefix = "snowflake.namingPrefix"
	SnowflakeMode         = "snowflake.mode"
	SnowflakeFormat       = "format"
)
