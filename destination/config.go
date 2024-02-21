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
	"github.com/conduitio-labs/conduit-connector-snowflake/config"
)

//go:generate paramgen -output=config_paramgen.go Config

type Config struct {
	config.Config
	// Username for the snowflake connection
	Username string `json:"snowflake.username" validate:"required"`
	// Password for the snowflake connection
	Password string `json:"snowflake.password" validate:"required"`
	// Host for the snowflake connection
	Host string `json:"snowflake.host" validate:"required"`
	// Port for the snowflake connection
	Port int `json:"snowflake.port" validate:"required"`
	// Database for the snowflake connection
	Database string `json:"snowflake.database" validate:"required"`
	// Schema for the snowflake connection
	Schema string `json:"snowflake.schema" validate:"required"`
	// Warehouse for the snowflake connection
	Warehouse string `json:"snowflake.warehouse" validate:"required"`
	// Whether to keep the session alive even when the connection is idle.
	KeepAlive bool `json:"snowflake.keepAlive" default:"true"`
	// Snowflake Stage to use for uploading files before merging into destination table.
	Stage string `json:"snowflake.stage" validate:"required"`
	// Primary key of the source table
	PrimaryKey string `json:"snowflake.primaryKey" validate:"required"`
	// Prefix to append to update_at , deleted_at, create_at at destination table
	NamingPrefix string `json:"snowflake.namingPrefix" default:"meroxa"`
	// Data type of file we upload and copy data from to snowflake
	Format string `json:"snowflake.format" default:"csv" validate:"required,inclusion=csv"`
	// For CSV processing, the number of goroutines to concurrently process CSV rows.
	CSVGoroutines int `json:"snowflake.csvGoroutines" default:"1"`
	// Number of threads to run for PUT file uploads.
	FileUploadThreads int `json:"snowflake.fileUploadThreads" default:"30"`
	// Compression to use when staging files in Snowflake
	Compression string `json:"snowflake.compression" default:"gzip" validate:"required,inclusion=gzip|zstd|copy"`
}

const (
	SnowflakeStage             = "snowflake.stage"
	SnowflakePrimaryKey        = "snowflake.primaryKey"
	SnowflakeNamingPrefix      = "snowflake.namingPrefix"
	SnowflakeFormat            = "snowflake.format"
	SnowflakeCSVGoRoutines     = "snowflake.csvGoroutines"
	SnowflakeFileUploadThreads = "snowflake.fileUploadThreads"
)
