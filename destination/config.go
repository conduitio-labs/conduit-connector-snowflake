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
	"bytes"
	"fmt"
	"github.com/Masterminds/sprig/v3"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"strings"
	"text/template"
)

//go:generate paramgen -output=config_paramgen.go Config

type TableFn func(sdk.Record) (string, error)

type Config struct {
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
	// Destination snowflake table. Support Go Templates for multiple collections.
	Table string `json:"table" default:"{{ index .Metadata \"opencdc.collection\" }}"`
	// Warehouse for the snowflake connection
	Warehouse string `json:"snowflake.warehouse" validate:"required"`
	// Whether to keep the session alive even when the connection is idle.
	KeepAlive bool `json:"snowflake.keepAlive" default:"true"`
	// Snowflake Stage to use for uploading files before merging into destination table.
	Stage string `json:"snowflake.stage" validate:"required"`
	// Primary key of the destination table.
	PrimaryKey string `json:"snowflake.primaryKey" validate:"required"`
	// Prefix to append to updated_at , deleted_at, created_at at destination table
	NamingPrefix string `json:"snowflake.namingPrefix" default:"meroxa" validate:"required"`
	// Data type of file we upload and copy data from to snowflake
	Format string `json:"snowflake.format" default:"csv" validate:"required,inclusion=csv"`
	// For CSV processing, the number of goroutines to concurrently process CSV rows.
	ProcessingWorkers int `json:"snowflake.processingWorkers" default:"1"`
	// Number of threads to run for PUT file uploads.
	FileUploadThreads int `json:"snowflake.fileUploadThreads" default:"30"`
	// Compression to use when staging files in Snowflake
	Compression string `json:"snowflake.compression" default:"zstd" validate:"required,inclusion=gzip|zstd|copy"`
}

// TableFunction returns a function that determines the table for each record individually.
// The function might be returning a static table name.
// If the table is neither static nor a template, an error is returned.
func (c Config) TableFunction() (f TableFn, err error) {
	// Not a template, i.e. it's a static table name
	if !strings.HasPrefix(c.Table, "{{") && !strings.HasSuffix(c.Table, "}}") {
		return func(_ sdk.Record) (string, error) {
			return c.Table, nil
		}, nil
	}

	// Try to parse the table
	t, err := template.New("table").Funcs(sprig.FuncMap()).Parse(c.Table)
	if err != nil {
		// The table is not a valid Go template.
		return nil, fmt.Errorf("table is neither a valid static table nor a valid Go template: %w", err)
	}

	// The table is a valid template, return TableFn.
	var buf bytes.Buffer
	return func(r sdk.Record) (string, error) {
		buf.Reset()
		if err := t.Execute(&buf, r); err != nil {
			return "", fmt.Errorf("failed to execute table template: %w", err)
		}
		return buf.String(), nil
	}, nil
}
