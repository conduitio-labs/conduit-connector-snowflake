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

package snowflake

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-snowflake/config"
)

type Spec struct{}

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "snowflake",
		Summary: "An Snowflake source plugin for Conduit, written in Go.",
		Version: "v0.1.0",
		Author:  "Meroxa, Inc.",
		SourceParams: map[string]sdk.Parameter{
			config.KeyConnection: {
				Default:     "",
				Required:    true,
				Description: "Snowflake connection string.",
			},
			config.KeyTable: {
				Default:     "",
				Required:    true,
				Description: "The table name that the connector should read.",
			},
			config.KeyColumns: {
				Default:     "",
				Required:    false,
				Description: "Comma separated list of column names that should be included in each Record's payload.",
			},
			config.KeyKey: {
				Default:     "",
				Required:    true,
				Description: "Column name that records should use for their `Key` fields.",
			},
		},
	}
}
