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
	"errors"
	"strconv"
	"strings"
)

const (
	KeyConnection string = "connection"
	KeyTable      string = "table"
	KeyColumns    string = "columns"
	KeyKey        string = "key"
	KeyLimit      string = "limit"

	DefaultLimit = 1000
)

// Config represents configuration needed for Snowflake.
type Config struct {
	Connection string `validate:"required"`
	Table      string `validate:"required,max=255"`
	Columns    []string
	Key        string `validate:"required,max=251"`
	Limit      int    `validate:"gte=1,lte=100000"`
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

	if cfg[KeyLimit] == "" {
		config.Limit = DefaultLimit

		return config, config.Validate()
	}

	limit, err := strconv.Atoi(cfg[KeyLimit])
	if err != nil {
		return Config{}, errors.New(`"limit" config value must be int`)
	}

	config.Limit = limit

	// Columns in snowflake is uppercase.
	if cfg[KeyKey] != "" {
		config.Key = strings.ToUpper(config.Key)
	}

	return config, config.Validate()
}
