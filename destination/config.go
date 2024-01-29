package destination

import "github.com/conduitio-labs/conduit-connector-snowflake/config"

//go:generate paramgen -output=config_paramgen.go Config

type Config struct {
	config.Config
	// Snowflake Stage data is copied into before merge
	Stage string `json:"snowflake.stage" default:"0"`
}

const (
	SnowflakeStage = "snowflake.stage"
)
