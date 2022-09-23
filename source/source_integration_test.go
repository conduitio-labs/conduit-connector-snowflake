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

package source

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
)

const (
	testTable         = "CONDUIT_INTEGRATION_TEST_TABLE"
	testTrackingTable = "CONDUIT_TRACKING_CONDUIT_INTEGRATION_TEST_TABLE"

	queryCreateTable        = "CREATE OR REPLACE TABLE %s (ID INT, NAME STRING)"
	queryInsertSnapshotData = "INSERT INTO %s VALUES (1, 'Petro'), (2, 'Olena')"
	queryDeleteTable        = "DROP TABLE %s"
	queryDeleteRow          = "DELETE FROM %s WHERE ID = 1"
	queryUpdateRow          = "UPDATE %s set NAME = 'test' WHERE ID = 2"
)

func TestSource_Snapshot(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareData(ctx, cfg["connection"])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg["connection"]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check first read.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var wantedKey sdk.StructuredData
	wantedKey = map[string]interface{}{"ID": "1"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Fatal(errors.New("wrong record key"))
	}

	// Check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Start from previous position.
	err = s.Open(ctx, r.Position)
	if err != nil {
		t.Fatal(err)
	}

	// Check read after teardown.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedKey = map[string]interface{}{"ID": "2"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Fatal(errors.New("wrong record key"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_CDC(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareData(ctx, cfg["connection"])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg["connection"]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot case.
	_, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot case.
	_, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = prepareCDCData(ctx, cfg["connection"])
	if err != nil {
		t.Fatal(err)
	}

	// Check cdc update.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if r.Operation != sdk.OperationUpdate {
		t.Fatal(errors.New("wrong action"))
	}

	// Check cdc delete.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if r.Operation != sdk.OperationDelete {
		t.Fatal(errors.New("wrong action"))
	}
}

func TestSource_Snapshot_Empty_Table(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareEmptyTable(ctx, cfg["connection"])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg["connection"]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check read from empty table.
	_, err = s.Read(ctx)
	if err != sdk.ErrBackoffRetry {
		t.Fatal(err)
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_CDC_Empty_Stream(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareData(ctx, cfg["connection"])
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cfg["connection"]) // nolint:errcheck,nolintlint

	s := new(Source)

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot case.
	_, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Snapshot case.
	_, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	err = prepareCDCData(ctx, cfg["connection"])
	if err != nil {
		t.Fatal(err)
	}

	// CDC case.
	_, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// CDC case.
	_, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// CDC read from empty stream.
	_, err = s.Read(ctx)
	if err != sdk.ErrBackoffRetry {
		t.Fatal(err)
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareConfig() (map[string]string, error) {
	connection := os.Getenv("SNOWFLAKE_CONNECTION")

	if connection == "" {
		return map[string]string{}, errors.New("SNOWFLAKE_CONNECTION env var must be set")
	}

	return map[string]string{
		config.KeyConnection:     connection,
		config.KeyTable:          testTable,
		config.KeyColumns:        "",
		config.KeyPrimaryKey:     "id",
		config.KeyOrderingColumn: "id",
	}, nil
}

func prepareData(ctx context.Context, conn string) error {
	db, err := sql.Open("snowflake", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTable, testTable))
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryInsertSnapshotData, testTable))
	if err != nil {
		return err
	}

	return nil
}

func prepareEmptyTable(ctx context.Context, conn string) error {
	db, err := sql.Open("snowflake", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryCreateTable, testTable))
	if err != nil {
		return err
	}

	return nil
}

func prepareCDCData(ctx context.Context, conn string) error {
	db, err := sql.Open("snowflake", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryDeleteRow, testTable))
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryUpdateRow, testTable))
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, conn string) error {
	db, err := sql.Open("snowflake", conn)
	if err != nil {
		return err
	}

	defer db.Close()

	err = db.PingContext(ctx)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryDeleteTable, testTable))
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf(queryDeleteTable, testTrackingTable))
	if err != nil {
		return err
	}

	return nil
}
