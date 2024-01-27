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

// Config represents configuration needed for Snowflake.
type Config struct {
	// Connection string connection to snowflake DB.
	// Detail information https://pkg.go.dev/github.com/snowflakedb/gosnowflake@v1.6.9#hdr-Connection_String
	Connection string `json:"snowflake.url" validate:"required"`
	// Table name.
	Table string `json:"snowflake.table" validate:"required"`
}

const (
	// KeyConnection - string for connect to db2.
	KeyConnection string = "snowflake.url"
	// KeyTable database table name.
	KeyTable string = "snowflake.table"
)
