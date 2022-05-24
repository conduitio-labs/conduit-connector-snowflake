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
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-snowflake/config"
	"github.com/conduitio/conduit-connector-snowflake/source"
	"go.uber.org/goleak"
)

func TestAcceptance(t *testing.T) {
	username := os.Getenv("SNOWFLAKE_USERNAME")
	if username == "" {
		t.Skip("SNOWFLAKE_USERNAME env var must be set")
	}

	password := os.Getenv("SNOWFLAKE_PASSWORD")
	if password == "" {
		t.Skip("SNOWFLAKE_PASSWORD env var must be set")
	}

	host := os.Getenv("SNOWFLAKE_HOST")
	if host == "" {
		t.Skip("SNOWFLAKE_HOST env var must be set")
	}

	database := os.Getenv("SNOWFLAKE_DATABASE")
	if database == "" {
		t.Skip("SNOWFLAKE_DATABASE env var must be set")
	}

	schema := os.Getenv("SNOWFLAKE_SCHEMA")
	if schema == "" {
		t.Skip("SNOWFLAKE_SCHEMA env var must be set")
	}

	connectionURL := fmt.Sprintf("%s:%s@%s/%s/%s", username, password, host, database, schema)

	table := setupTestDB(t, connectionURL)

	cfg := map[string]string{
		config.KeyConnection: connectionURL,
		config.KeyTable:      table,
		config.KeyKey:        "id",
	}

	sdk.AcceptanceTest(t, sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: Specification,
				NewSource:        source.New,
				NewDestination:   nil,
			},
			SourceConfig:      cfg,
			DestinationConfig: nil,
			Skip: []string{
				// the method requires NewDestination and has unstructured data
				"TestSource_Read*",
				// the method requires NewDestination
				"TestSource_Open_ResumeAtPositionSnapshot",
			},
			GoleakOptions: []goleak.Option{
				goleak.IgnoreTopFunction("database/sql.(*DB).connectionOpener"),
				goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
				goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
			},
			BeforeTest: func(t *testing.T) {
				t.Logf("testing on table %s", table)
			},
		},
	},
	)
}

func setupTestDB(t *testing.T, connectionURL string) string {
	db, err := sql.Open("snowflake", connectionURL)
	if err != nil {
		t.Errorf("open db: %v", err)
	}

	err = db.PingContext(context.Background())
	if err != nil {
		t.Errorf("ping db: %v", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Errorf("create conn: %v", err)
	}

	tableName := randomIdentifier(t)

	createQuery := fmt.Sprintf("create or replace table %s (id int, name text);", tableName)

	_, err = conn.ExecContext(context.Background(), createQuery)
	if err != nil {
		t.Errorf("create test table: %v", err)
	}

	t.Cleanup(func() {
		dropQuery := fmt.Sprintf("drop table %s;", tableName)
		_, err = conn.ExecContext(context.Background(), dropQuery)
		if err != nil {
			t.Errorf("drop test table: %v", err)
		}
	})

	return tableName
}

func randomIdentifier(t *testing.T) string {
	return fmt.Sprintf("conduit_%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000)
}
