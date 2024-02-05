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

import (
	"testing"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestParseConfig(t *testing.T) {
	exampleConfig := map[string]string{}

	want := Config{
		Config: config.Config{
			Connection: "",
			Table:      "",
		},
		Columns:        nil,
		Keys:           nil,
		OrderingColumn: "",
		BatchSize:      0,
		Snapshot:       false,
	}

	is := is.New(t)
	var got Config
	err := sdk.Util.ParseConfig(exampleConfig, &got)

	is.NoErr(err)
	is.Equal(want, got)
}
