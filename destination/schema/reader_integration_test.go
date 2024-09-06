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

package schema

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	//"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
	"github.com/snowflakedb/gosnowflake"
)

func Test_SHOW(t *testing.T) {
	utcTZ := "UTC"
	is := is.New(t)

	config := &gosnowflake.Config{
		Account:          "cr90602",
		User:             os.Getenv("SNOWSQL_USER"),
		Password:         os.Getenv("SNOWSQL_PWD"),
		Host:             os.Getenv("SNOWSQL_HOST"),
		Port:             443,
		Database:         os.Getenv("SNOWSQL_DATABASE"),
		Schema:           os.Getenv("SNOWSQL_SCHEMA"),
		Warehouse:        "compute_wh",
		KeepSessionAlive: true,
		Params: map[string]*string{
			"TIMEZONE": &utcTZ,
		},
	}

	dsn, err := gosnowflake.DSN(config)
	is.NoErr(err)

	t.Logf("url: %s", dsn)
}

func TestTableParser_Parse(t *testing.T) {
	dsn := os.Getenv("SNOWFLAKE_DSN")
	if dsn == "" {
		t.Skipf("SNOWFLAKE_DSN is not provided, skipping test")
	}

	is := is.New(t)

	db, err := sql.Open("snowflake", dsn)
	is.NoErr(err)

	table := setupTable(t, db)

	p := newTableParser(db)

	sch, err := p.Parse(context.Background(), table)
	is.NoErr(err)

	expected := Schema{
		"ID": Column{
			Name:      "ID",
			Type:      Number,
			Precision: 38,
			Scale:     0,
			Varlen:    -1,
		},
		"BALANCE": Column{
			Name:      "BALANCE",
			Type:      Numeric,
			Precision: 10,
			Scale:     2,
			Varlen:    -1,
		},
		"LENGTH": Column{
			Name:      "LENGTH",
			Type:      Float,
			Precision: -1,
			Scale:     -1,
			Varlen:    -1,
		},
		"DESC": Column{
			Name:      "DESC",
			Type:      Text,
			Precision: -1,
			Scale:     -1,
			Varlen:    16777216,
		},
		"CREATED_AT": Column{
			Name:      "CREATED_AT",
			Type:      TimestampTZ,
			Precision: -1,
			Scale:     -1,
			Varlen:    -1,
		},
	}

	is.True(sch.Eq(expected))

	//is.Equal("", cmp.Diff(sch, expected))
}

func setupTable(t *testing.T, db *sql.DB) string {
	is := is.New(t)
	tableName := strings.ToUpper(randomId(t))

	_, err := db.Exec(fmt.Sprintf(`CREATE TABLE %s (
		ID INT,
		BALANCE NUMERIC(10,2),
		LENGTH FLOAT,
		DESC TEXT,
		CREATED_AT TIMESTAMP_TZ
	)`, tableName))

	is.NoErr(err)
	t.Cleanup(func() {
		_, err := db.Exec("DROP TABLE " + tableName)
		is.NoErr(err)
	})

	return tableName
}

func randomId(t *testing.T) string {
	return fmt.Sprintf("conduit_%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000)
}
