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

package writer

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/conduitio-labs/conduit-connector-snowflake/destination/compress"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/stretchr/testify/require"
)

func TestWriter_Close(t *testing.T) {
	testCases := []struct {
		desc        string
		stage       string
		dbmock      func() (*sql.DB, sqlmock.Sqlmock)
		expectedErr error
	}{
		{
			desc:  "success",
			stage: "testStage",
			dbmock: func() (*sql.DB, sqlmock.Sqlmock) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)

				mock.ExpectExec("DROP STAGE testStage").
					WillReturnResult(sqlmock.NewResult(0, 1)).
					WillReturnError(nil)

				mock.ExpectClose()

				return db, mock
			},
		},
		{
			desc:  "failed to drop stage",
			stage: "testStage",
			dbmock: func() (*sql.DB, sqlmock.Sqlmock) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)

				mock.ExpectExec("DROP STAGE testStage").
					WillReturnError(errors.New("test error"))

				return db, mock
			},
			expectedErr: errors.New("failed to drop stage: test error"),
		},
		{
			desc:  "failed to close db",
			stage: "testStage",
			dbmock: func() (*sql.DB, sqlmock.Sqlmock) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)

				mock.ExpectExec("DROP STAGE testStage").
					WillReturnResult(sqlmock.NewResult(0, 1)).
					WillReturnError(nil)

				mock.ExpectClose().
					WillReturnError(errors.New("test error"))

				return db, mock
			},
			expectedErr: errors.New("ailed to gracefully close the connection: test error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			db, mock := tc.dbmock()
			s := SnowflakeCSV{
				config: SnowflakeConfig{
					Stage: tc.stage,
				},
				db: db,
			}

			err := s.Close(ctx)
			if tc.expectedErr != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
				require.NoError(t, mock.ExpectationsWereMet())
			}
		})
	}
}

// TODO: add error test cases, this just covers the success case at the moment.
func TestWriter_Write(t *testing.T) {
	testTimestamp := time.Now().UnixMilli()
	testCases := []struct {
		desc              string
		Prefix            string
		PrimaryKey        string
		Stage             string
		Table             string
		FileThreads       int
		ProcessingWorkers int
		compressor        compress.Compressor
		records           []sdk.Record
		dbmock            func() (*sql.DB, sqlmock.Sqlmock)
		expectedErr       error
	}{
		{
			desc:              "successful batch",
			Table:             "test",
			PrimaryKey:        "id",
			Stage:             "test-stage",
			Prefix:            "meroxa",
			ProcessingWorkers: 1,
			FileThreads:       1,
			compressor:        compress.Copy{},
			records: []sdk.Record{
				{
					Position:  sdk.Position("1"),
					Operation: sdk.OperationCreate,
					Metadata: sdk.Metadata{
						"opencdc.readAt": fmt.Sprint(testTimestamp),
					},
					Key: sdk.StructuredData{
						"id": "1",
					},
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":        "1",
							"firstName": "spongebob",
							"lastName":  "squarepants",
						},
					},
				},
				{
					Position:  sdk.Position("2"),
					Operation: sdk.OperationCreate,
					Metadata: sdk.Metadata{
						"opencdc.readAt": fmt.Sprint(testTimestamp),
					},
					Key: sdk.StructuredData{
						"id": "2",
					},
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":        "2",
							"firstName": "patrick",
							"lastName":  "star",
						},
					},
				},
				{
					Position:  sdk.Position("3"),
					Operation: sdk.OperationUpdate,
					Metadata: sdk.Metadata{
						"opencdc.readAt": fmt.Sprint(testTimestamp),
					},
					Key: sdk.StructuredData{
						"id": "3",
					},
					Payload: sdk.Change{
						After: sdk.StructuredData{
							"id":        "3",
							"firstName": "squidward",
							"lastName":  "tentacles",
						},
					},
				},
				{
					Position:  sdk.Position("4"),
					Operation: sdk.OperationDelete,
					Metadata: sdk.Metadata{
						"opencdc.readAt": fmt.Sprint(testTimestamp),
					},
					Key: sdk.StructuredData{
						"id": "4",
					},
					Payload: sdk.Change{
						Before: sdk.StructuredData{
							"id":        "4",
							"firstName": "eugene",
							"lastName":  "krabs",
						},
					},
				},
			},
			dbmock: func() (*sql.DB, sqlmock.Sqlmock) {
				db, mock, err := sqlmock.New()
				require.NoError(t, err)

				// Check if table exist and return 0 rows to check `SetupTables` is called
				mock.ExpectQuery(`SHOW TABLES LIKE 'test'`).
					WillReturnRows(sqlmock.NewRows([]string{}))

				mock.ExpectBegin()

				mock.ExpectExec(`CREATE TABLE IF NOT EXISTS test \(
						meroxa_operation VARCHAR,
						meroxa_created_at TIMESTAMP_TZ,
						meroxa_updated_at TIMESTAMP_TZ,
						meroxa_deleted_at TIMESTAMP_TZ,
						firstName VARCHAR,
						id VARCHAR,
						lastName VARCHAR,
						PRIMARY KEY \(id\)
					\)`).
					WillReturnResult(sqlmock.NewResult(1, 1)).
					WillReturnError(nil)

				mock.ExpectCommit()

				mock.ExpectExec(`
				PUT file://.*_inserts.csv.copy @test-stage SOURCE_COMPRESSION=copy PARALLEL=1
				`).
					WillReturnResult(sqlmock.NewResult(2, 2)).
					WillReturnError(nil)

				mock.ExpectExec(`
				PUT file://.*_updates.csv.copy @test-stage SOURCE_COMPRESSION=copy PARALLEL=1
				`).
					WillReturnResult(sqlmock.NewResult(4, 2)).
					WillReturnError(nil)

				mock.ExpectBegin()

				// TODO: assert matches on subqueries separately. this is cumbersome.
				mock.ExpectExec(`
				MERGE INTO test as a USING \(
					select \$1 meroxa_operation, \$2 meroxa_created_at, \$3 meroxa_updated_at,
						\$4 meroxa_deleted_at, \$5 firstName, \$6 id, \$7 lastName
					from @test-stage/.*_inserts.csv.copy \(FILE_FORMAT =>  CSV_CONDUIT_SNOWFLAKE \)
				\) AS b ON a.id = b.id
				WHEN MATCHED AND \( b.meroxa_operation = 'create' OR b.meroxa_operation = 'snapshot' \)
					THEN UPDATE SET a.meroxa_operation = b.meroxa_operation, a.meroxa_created_at = b.meroxa_created_at,
					a.meroxa_updated_at = b.meroxa_updated_at, a.meroxa_deleted_at = b.meroxa_deleted_at, a.firstName = b.firstName,
					a.id = b.id, a.lastName = b.lastName
				WHEN NOT MATCHED AND \( b.meroxa_operation = 'create' OR b.meroxa_operation = 'snapshot' \)
					THEN INSERT  \(a.meroxa_operation, a.meroxa_created_at, a.meroxa_updated_at,
						a.meroxa_deleted_at, a.firstName, a.id, a.lastName\)
					VALUES \(b.meroxa_operation, b.meroxa_created_at, b.meroxa_updated_at,
						b.meroxa_deleted_at, b.firstName, b.id, b.lastName\) ;
				`).
					WillReturnResult(sqlmock.NewResult(2, 2)).
					WillReturnError(nil)

				mock.ExpectExec(`
					MERGE INTO test as a USING \(
						select \$1 meroxa_operation, \$2 meroxa_created_at, \$3 meroxa_updated_at,
							\$4 meroxa_deleted_at, \$5 firstName, \$6 id, \$7 lastName
						from @test-stage/.*_updates.csv.copy \(FILE_FORMAT =>  CSV_CONDUIT_SNOWFLAKE \)
					\) AS b ON a.id = b.id
					WHEN MATCHED AND b.meroxa_operation = 'update'
						THEN UPDATE SET a.meroxa_operation = b.meroxa_operation, a.meroxa_updated_at = b.meroxa_updated_at,
						a.meroxa_deleted_at = b.meroxa_deleted_at, a.firstName = b.firstName, a.id = b.id, a.lastName = b.lastName
					WHEN MATCHED AND b.meroxa_operation = 'delete'
						THEN UPDATE SET a.meroxa_operation = b.meroxa_operation, a.meroxa_deleted_at = b.meroxa_deleted_at
					WHEN NOT MATCHED AND b.meroxa_operation = 'update'
						THEN INSERT  \(a.meroxa_operation, a.meroxa_created_at, a.meroxa_updated_at,
							a.meroxa_deleted_at, a.firstName, a.id, a.lastName\)
						VALUES \(b.meroxa_operation, b.meroxa_created_at, b.meroxa_updated_at,
							b.meroxa_deleted_at, b.firstName, b.id, b.lastName\)
					WHEN NOT MATCHED AND b.meroxa_operation = 'delete'
						THEN INSERT  \(a.meroxa_operation, a.meroxa_created_at, a.meroxa_updated_at,
							 a.meroxa_deleted_at, a.firstName, a.id, a.lastName\)
						VALUES \(b.meroxa_operation, b.meroxa_created_at, b.meroxa_updated_at,
							b.meroxa_deleted_at, b.firstName, b.id, b.lastName\) ;
					`).
					WillReturnResult(sqlmock.NewResult(4, 2)).
					WillReturnError(nil)

				mock.ExpectCommit()

				return db, mock
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()

			db, mock := tc.dbmock()
			defer db.Close()
			s := &SnowflakeCSV{
				config: SnowflakeConfig{
					Prefix:            tc.Prefix,
					PrimaryKey:        tc.PrimaryKey,
					Stage:             tc.Stage,
					Table:             tc.Table,
					FileThreads:       tc.FileThreads,
					ProcessingWorkers: tc.ProcessingWorkers,
				},
				db:         db,
				compressor: tc.compressor,

				insertsBuf:    &bytes.Buffer{},
				updatesBuf:    &bytes.Buffer{},
				compressedBuf: &bytes.Buffer{},
			}

			n, err := s.Write(ctx, tc.records)

			if tc.expectedErr != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, len(tc.records), n)
				require.NoError(t, mock.ExpectationsWereMet())
			}
		})
	}
}
