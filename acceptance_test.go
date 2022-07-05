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
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	builder "github.com/huandu/go-sqlbuilder"
	"go.uber.org/goleak"

	"github.com/conduitio/conduit-connector-snowflake/config"
	"github.com/conduitio/conduit-connector-snowflake/source"
	"github.com/conduitio/conduit-connector-snowflake/source/iterator"
)

// ConfigurableAcceptanceTestDriver driver for test.
type ConfigurableAcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

// WriteToSource - write data to table.
func (d ConfigurableAcceptanceTestDriver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
	connectionURL := os.Getenv("SNOWFLAKE_CONNECTION_URL")

	db, err := sql.Open("snowflake", connectionURL)
	if err != nil {
		t.Errorf("open db: %v", err)
	}

	defer db.Close()

	err = db.PingContext(context.Background())
	if err != nil {
		t.Errorf("ping db: %v", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Errorf("create conn: %v", err)
	}

	defer conn.Close()

	for _, r := range records {
		er := writeRecord(conn, r, d.Config.SourceConfig[config.KeyTable])
		if er != nil {
			t.Errorf("write to snowflake %s", err)
		}
	}

	return records
}

// GenerateRecord generate record for snowflake account.
func (d ConfigurableAcceptanceTestDriver) GenerateRecord(t *testing.T) sdk.Record {
	id := uuid.New().String()
	m := map[string]any{"ID": id}

	b, err := json.Marshal(m)
	if err != nil {
		t.Error(err)
	}

	return sdk.Record{
		Position:  sdk.Position(uuid.New().String()),
		Metadata:  nil,
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			d.Config.SourceConfig[config.KeyPrimaryKey]: id,
		},
		Payload: sdk.RawData(b),
	}
}

func TestAcceptance(t *testing.T) {
	connectionURL := os.Getenv("SNOWFLAKE_CONNECTION_URL")
	if connectionURL == "" {
		t.Skip("SNOWFLAKE_CONNECTION_URL env var must be set")
	}

	table := setupTestDB(t, connectionURL)

	cfg := map[string]string{
		config.KeyConnection: connectionURL,
		config.KeyTable:      table,
		config.KeyPrimaryKey: "ID",
	}

	sdk.AcceptanceTest(t, ConfigurableAcceptanceTestDriver{sdk.ConfigurableAcceptanceTestDriver{
		Config: sdk.ConfigurableAcceptanceTestDriverConfig{
			Connector: sdk.Connector{
				NewSpecification: Specification,
				NewSource:        source.New,
				NewDestination:   nil,
			},
			SourceConfig:      cfg,
			DestinationConfig: nil,
			GoleakOptions: []goleak.Option{
				// Snowflake driver has those leaks. Issue: https://github.com/snowflakedb/gosnowflake/issues/588
				goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
				goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
			},
			BeforeTest: func(t *testing.T) {
				clearTable(t, cfg[config.KeyTable])
			},
		},
	}},
	)
}

func setupTestDB(t *testing.T, connectionURL string) string {
	db, err := sql.Open("snowflake", connectionURL)
	if err != nil {
		t.Errorf("open db: %v", err)
	}

	defer db.Close()

	err = db.PingContext(context.Background())
	if err != nil {
		t.Errorf("ping db: %v", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Errorf("create conn: %v", err)
	}

	defer conn.Close()

	tableName := randomIdentifier(t)

	createQuery := fmt.Sprintf("create or replace table %s (id string);", tableName)

	_, err = conn.ExecContext(context.Background(), createQuery)
	if err != nil {
		t.Errorf("create test table: %v", err)
	}

	t.Cleanup(func() {
		d, er := sql.Open("snowflake", connectionURL)
		if er != nil {
			t.Error(er)
		}

		defer d.Close()

		dropQuery := fmt.Sprintf("drop table %s;", tableName)
		_, err = d.ExecContext(context.Background(), dropQuery)
		if err != nil {
			t.Errorf("drop test table: %v", err)
		}

		trackingTable := fmt.Sprintf("%s_tracking_%s", iterator.Conduit, tableName)

		dropTrackingTable := fmt.Sprintf("drop table %s;", trackingTable)
		_, err = d.ExecContext(context.Background(), dropTrackingTable)
		if err != nil {
			t.Errorf("drop tracking table: %v", err)
		}
	})

	return tableName
}

func randomIdentifier(t *testing.T) string {
	return fmt.Sprintf("conduit_%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000)
}

func writeRecord(conn *sql.Conn, r sdk.Record, table string) error {
	payload, err := structurizeData(r.Payload)
	if err != nil {
		return fmt.Errorf("structurize data")
	}

	cols, vals := extractColumnsAndValues(payload)

	sqlbuilder := builder.NewInsertBuilder()
	sqlbuilder.InsertInto(table)
	sqlbuilder.Cols(cols...)
	sqlbuilder.Values(vals...)
	q, arg := sqlbuilder.Build()

	_, err = conn.ExecContext(context.Background(), q, arg...)

	return err
}

func extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
	var (
		colArgs []string
		valArgs []any
	)

	for field, value := range payload {
		colArgs = append(colArgs, field)
		valArgs = append(valArgs, value)
	}

	return colArgs, valArgs
}

func structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	err := json.Unmarshal(data.Bytes(), &structuredData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal data into structured data: %w", err)
	}

	structuredDataUpper := make(sdk.StructuredData)
	for key, value := range structuredData {
		if parsedValue, ok := value.(map[string]any); ok {
			valueJSON, err := json.Marshal(parsedValue)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal map into json: %w", err)
			}

			structuredDataUpper[strings.ToUpper(key)] = string(valueJSON)

			continue
		}

		structuredDataUpper[strings.ToUpper(key)] = value
	}

	return structuredDataUpper, nil
}

func clearTable(t *testing.T, table string) {
	connectionURL := os.Getenv("SNOWFLAKE_CONNECTION_URL")

	db, err := sql.Open("snowflake", connectionURL)
	if err != nil {
		t.Errorf("open db: %v", err)
	}

	defer db.Close()

	err = db.PingContext(context.Background())
	if err != nil {
		t.Errorf("ping db: %v", err)
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Errorf("create conn: %v", err)
	}

	defer conn.Close()

	_, err = conn.ExecContext(context.Background(), fmt.Sprintf("TRUNCATE TABLE %s", table))
	if err != nil {
		t.Errorf("trucate table: %v", err)
	}
}
