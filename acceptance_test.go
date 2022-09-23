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
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	builder "github.com/huandu/go-sqlbuilder"
	"go.uber.org/goleak"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	"github.com/conduitio-labs/conduit-connector-snowflake/source/iterator"
)

// driver Configurable Acceptance test driver.
type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	counter int32
}

// WriteToSource - write data to table.
func (d *driver) WriteToSource(t *testing.T, records []sdk.Record) []sdk.Record {
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
func (d *driver) GenerateRecord(t *testing.T, operation sdk.Operation) sdk.Record {
	atomic.AddInt32(&d.counter, 1)

	m := map[string]any{"ID": fmt.Sprintf("%d", d.counter)}

	b, err := json.Marshal(m)
	if err != nil {
		t.Error(err)
	}

	return sdk.Record{
		Position:  sdk.Position(uuid.New().String()),
		Operation: operation,
		Key: sdk.StructuredData{
			"ID": fmt.Sprintf("%d", d.counter),
		},
		Payload: sdk.Change{
			Before: nil,
			After:  sdk.RawData(b),
		},
	}
}

func TestAcceptance(t *testing.T) {
	connectionURL := os.Getenv("SNOWFLAKE_CONNECTION_URL")
	if connectionURL == "" {
		t.Skip("SNOWFLAKE_CONNECTION_URL env var must be set")
	}

	cfg := map[string]string{
		config.KeyConnection:     connectionURL,
		config.KeyPrimaryKey:     "ID",
		config.KeyOrderingColumn: "ID",
	}

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(t, cfg),
				GoleakOptions: []goleak.Option{
					// Snowflake driver has those leaks. Issue: https://github.com/snowflakedb/gosnowflake/issues/588
					goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
				},
			},
		},
	})
}

func setupTestDB(t *testing.T, connectionURL, table string) error {
	db, err := sql.Open("snowflake", connectionURL)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(context.Background())
	if err != nil {
		return err
	}

	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}

	defer conn.Close()

	createQuery := fmt.Sprintf("create table %s (id text);", table)

	_, err = conn.ExecContext(context.Background(), createQuery)
	if err != nil {
		return err
	}

	t.Cleanup(func() {
		d, er := sql.Open("snowflake", connectionURL)
		if er != nil {
			t.Fatal(er)
		}

		defer d.Close()

		dropQuery := fmt.Sprintf("drop table %s;", table)

		_, err = d.ExecContext(context.Background(), dropQuery)
		if err != nil {
			t.Fatal(err)
		}

		trackingTable := fmt.Sprintf("%s_tracking_%s", iterator.Conduit, table)

		dropTrackingTable := fmt.Sprintf("drop table if exists  %s", trackingTable)
		_, err = d.ExecContext(context.Background(), dropTrackingTable)
		if err != nil {
			t.Fatal(err)
		}
	})

	return nil
}

func randomIdentifier(t *testing.T) string {
	return strings.ToUpper(fmt.Sprintf("%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000))
}

func writeRecord(conn *sql.Conn, r sdk.Record, table string) error {
	payload, err := structurizeData(r.Payload.After)
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

// beforeTest creates new table before each test.
func beforeTest(t *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		table := randomIdentifier(t)
		t.Logf("table under test: %v", table)

		cfg[config.KeyTable] = table

		err := setupTestDB(t, cfg[config.KeyConnection], table)
		if err != nil {
			t.Fatal(err)
		}
	}
}
