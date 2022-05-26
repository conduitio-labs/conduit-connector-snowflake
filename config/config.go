// Copyright © 2022 Meroxa, Inc.
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

package config

import (
	"strings"
)

const (
	KeyConnection string = "connection"
	KeyTable      string = "table"
	KeyColumns    string = "columns"
	KeyKey        string = "key"
)

// Config represents configuration needed for Snowflake.
type Config struct {
	// Connection string connection to snowflake DB.
	// Detail information https://pkg.go.dev/github.com/snowflakedb/gosnowflake@v1.6.9#hdr-Connection_String
	Connection string `validate:"required"`

	// Table name.
	Table string `validate:"required,max=255"`

	// List of columns from table, by default read all columns.
	Columns []string

	// Key - Column name that records should use for their `Key` fields.
	Key string `validate:"required,max=251"`
}

// Parse attempts to parse plugins.Config into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		Connection: cfg[KeyConnection],
		Table:      cfg[KeyTable],
		Key:        cfg[KeyKey],
	}

	if colsRaw := cfg[KeyColumns]; colsRaw != "" {
		config.Columns = strings.Split(colsRaw, ",")
	}

	// Columns in snowflake is uppercase.
	if cfg[KeyKey] != "" {
		config.Key = strings.ToUpper(config.Key)
	}

	err := config.Validate()
	if err != nil {
		return Config{}, err
	}

	return config, nil
}
