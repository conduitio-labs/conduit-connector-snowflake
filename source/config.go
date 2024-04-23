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

package source

import "github.com/conduitio-labs/conduit-connector-snowflake/config"

//go:generate paramgen -output=config_paramgen.go Config

type Config struct {
	config.Config
	// Connection string connection to snowflake DB.
	// Detail information https://pkg.go.dev/github.com/snowflakedb/gosnowflake@v1.6.9#hdr-Connection_String
	Connection string `json:"snowflake.url" validate:"required"`
	// Snapshot whether or not the plugin will take a snapshot of the entire table before starting cdc.
	Columns []string `json:"snowflake.columns" default:"false"`
	// Primary keys
	Keys []string `json:"snowflake.primaryKeys"`
	// OrderingColumn is a name of a column that the connector will use for ordering rows.
	OrderingColumn string `json:"snowflake.orderingColumn"`
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
