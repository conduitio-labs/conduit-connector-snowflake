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

package format

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/schema/snowflake"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/go-cmp/cmp"
	"github.com/matryer/is"
)

func Test_MakeCSVBytes(t *testing.T) {
	is := is.New(t)

	const avroSchema = `
		{
			"fields": [
				{
					"name": "firstName",
					"type": "string"
				},
				{
					"name": "lastName",
					"type": "string"
				},
				{
					"name": "id",
					"type": "int"
				}
			],
			"name": "conduit.postgres.users_small",
			"type": "record"
		}`

	testTimestamp := time.Now().UnixMicro()
	testCases := []struct {
		desc        string
		records     []sdk.Record
		table       snowflake.Table
		wantCSV     string
		expectedErr error
	}{
		{
			desc: "no duplicates",
			records: []sdk.Record{
				{
					Position:  sdk.Position("1"),
					Operation: sdk.OperationCreate,
					Metadata: sdk.Metadata{
						"conduit.source.connector.id": "pg-to-file:pg.in",
						"opencdc.collection":          "users_small",
						"opencdc.readAt":              fmt.Sprint(testTimestamp),
						"opencdc.version":             "v1",
						"postgres.avro.schema":        avroSchema,
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
						"conduit.source.connector.id": "pg-to-file:pg.in",
						"opencdc.collection":          "users_small",
						"opencdc.readAt":              fmt.Sprint(testTimestamp),
						"opencdc.version":             "v1",
						"postgres.avro.schema":        avroSchema,
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
						"conduit.source.connector.id": "pg-to-file:pg.in",
						"opencdc.collection":          "users_small",
						"opencdc.readAt":              fmt.Sprint(testTimestamp),
						"opencdc.version":             "v1",
						"postgres.avro.schema":        avroSchema,
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
						"conduit.source.connector.id": "pg-to-file:pg.in",
						"opencdc.collection":          "users_small",
						"opencdc.readAt":              fmt.Sprint(testTimestamp),
						"opencdc.version":             "v1",
						"postgres.avro.schema":        avroSchema,
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
			table: snowflake.Table{
				Name:     "foo",
				Database: "bar",
				Schema:   "baz",
				Columns: []snowflake.Column{
					{Name: "meroxa_operation", DataType: snowflake.DataTypeText{IsNullable: true}},
					{Name: "meroxa_created_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}},
					{Name: "meroxa_updated_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}},
					{Name: "meroxa_deleted_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}},
					{Name: "id", DataType: snowflake.DataTypeFixed{IsNullable: true, Precision: 38}},
					{Name: "firstName", DataType: snowflake.DataTypeText{IsNullable: true}},
					{Name: "lastName", DataType: snowflake.DataTypeText{IsNullable: true}},
				},
				PrimaryKeys: []snowflake.Column{
					{Name: "id", DataType: snowflake.DataTypeFixed{IsNullable: true, Precision: 38}},
				},
				Operation: snowflake.Column{Name: "meroxa_operation", DataType: snowflake.DataTypeText{IsNullable: true}},
				CreatedAt: snowflake.Column{Name: "meroxa_created_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}},
				UpdatedAt: snowflake.Column{Name: "meroxa_updated_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}},
				DeletedAt: snowflake.Column{Name: "meroxa_deleted_at", DataType: snowflake.DataTypeTimestampTz{IsNullable: true, Scale: 9}},
			},
			wantCSV: `meroxa_operation,meroxa_created_at,meroxa_updated_at,meroxa_deleted_at,id,firstName,lastName
create,` + fmt.Sprint(testTimestamp) + `,,,1,spongebob,squarepants
create,` + fmt.Sprint(testTimestamp) + `,,,2,patrick,star
update,,` + fmt.Sprint(testTimestamp) + `,,3,squidward,tentacles
delete,,,` + fmt.Sprint(testTimestamp) + `,4,eugene,krabs
`,
		},
		// {
		// 	desc: "multiple goroutines",
		// 	records: []sdk.Record{
		// 		{
		// 			Position:  sdk.Position("1"),
		// 			Operation: sdk.OperationCreate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "1",
		// 			},
		// 			Payload: sdk.Change{
		// 				After: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob",
		// 					"lastName":  "squarepants",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Position:  sdk.Position("2"),
		// 			Operation: sdk.OperationCreate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "2",
		// 			},
		// 			Payload: sdk.Change{
		// 				After: sdk.StructuredData{
		// 					"id":        "2",
		// 					"firstName": "patrick",
		// 					"lastName":  "star",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Position:  sdk.Position("3"),
		// 			Operation: sdk.OperationUpdate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "3",
		// 			},
		// 			Payload: sdk.Change{
		// 				After: sdk.StructuredData{
		// 					"id":        "3",
		// 					"firstName": "squidward",
		// 					"lastName":  "tentacles",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Position:  sdk.Position("4"),
		// 			Operation: sdk.OperationDelete,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "4",
		// 			},
		// 			Payload: sdk.Change{
		// 				Before: sdk.StructuredData{
		// 					"id":        "4",
		// 					"firstName": "eugene",
		// 					"lastName":  "krabs",
		// 				},
		// 			},
		// 		},
		// 	},
		// 	colOrder: []string{
		// 		"meroxa_operation", "meroxa_created_at", "meroxa_updated_at",
		// 		"meroxa_deleted_at", "id", "firstName", "lastName",
		// 	},
		// 	meroxaColumns: ConnectorColumns{
		// 		operationColumn: "meroxa_operation",
		// 		createdAtColumn: "meroxa_created_at",
		// 		updatedAtColumn: "meroxa_updated_at",
		// 		deletedAtColumn: "meroxa_deleted_at",
		// 	},
		// 	prefix:        "meroxa",
		// 	primaryKey:    "id",
		// 	numGoRoutines: 3,
		// 	expectedBuffers: func() (*bytes.Buffer, *bytes.Buffer) {
		// 		insertBuf, updateBuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)
		// 		insertWriter := csv.NewWriter(insertBuf)
		// 		updateWriter := csv.NewWriter(updateBuf)

		// 		err := insertWriter.Write([]string{"create", fmt.Sprint(testTimestamp), "", "", "1", "spongebob", "squarepants"})
		// 		require.NoError(t, err)
		// 		err = insertWriter.Write([]string{"create", fmt.Sprint(testTimestamp), "", "", "2", "patrick", "star"})
		// 		require.NoError(t, err)
		// 		insertWriter.Flush()

		// 		err = updateWriter.Write([]string{"update", "", fmt.Sprint(testTimestamp), "", "3", "squidward", "tentacles"})
		// 		require.NoError(t, err)
		// 		err = updateWriter.Write([]string{"delete", "", "", fmt.Sprint(testTimestamp), "4", "eugene", "krabs"})
		// 		require.NoError(t, err)
		// 		updateWriter.Flush()

		// 		return insertBuf, updateBuf
		// 	},
		// },
		// {
		// 	desc: "create, update, and delete for same ID",
		// 	records: []sdk.Record{
		// 		{
		// 			Position:  sdk.Position("1"),
		// 			Operation: sdk.OperationCreate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "1",
		// 			},
		// 			Payload: sdk.Change{
		// 				After: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob",
		// 					"lastName":  "squarepants",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Position:  sdk.Position("1"),
		// 			Operation: sdk.OperationUpdate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp + 1),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "1",
		// 			},
		// 			Payload: sdk.Change{
		// 				Before: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob",
		// 					"lastName":  "squarepants",
		// 				},
		// 				After: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob1",
		// 					"lastName":  "squarepants1",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Position:  sdk.Position("1"),
		// 			Operation: sdk.OperationDelete,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp + 2),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "1",
		// 			},
		// 			Payload: sdk.Change{
		// 				Before: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob1",
		// 					"lastName":  "squarepants1",
		// 				},
		// 			},
		// 		},
		// 	},
		// 	colOrder: []string{
		// 		"meroxa_operation", "meroxa_created_at", "meroxa_updated_at",
		// 		"meroxa_deleted_at", "id", "firstName", "lastName",
		// 	},
		// 	meroxaColumns: ConnectorColumns{
		// 		operationColumn: "meroxa_operation",
		// 		createdAtColumn: "meroxa_created_at",
		// 		updatedAtColumn: "meroxa_updated_at",
		// 		deletedAtColumn: "meroxa_deleted_at",
		// 	},
		// 	prefix:     "meroxa",
		// 	primaryKey: "id",
		// 	expectedBuffers: func() (*bytes.Buffer, *bytes.Buffer) {
		// 		insertBuf, updateBuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

		// 		updateWriter := csv.NewWriter(updateBuf)
		// 		// create and update timestamps are not represented in this scenario
		// 		// this is to prevent removing the timestamps from the merged records in snowflake
		// 		// TODO: adjust MERGE query to prevent erased created_at / updated_at, instead
		// 		// of doing this via CSV recordSummary manipulation
		// 		err := updateWriter.Write([]string{
		// 			"delete", "", "", fmt.Sprint(testTimestamp + 2), "1", "spongebob1", "squarepants1",
		// 		})
		// 		require.NoError(t, err)
		// 		updateWriter.Flush()

		// 		return insertBuf, updateBuf
		// 	},
		// 	numGoRoutines: 1,
		// },
		// {
		// 	desc: "duplicate creates for same ID",
		// 	records: []sdk.Record{
		// 		{
		// 			Position:  sdk.Position("1"),
		// 			Operation: sdk.OperationCreate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "1",
		// 			},
		// 			Payload: sdk.Change{
		// 				After: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob",
		// 					"lastName":  "squarepants",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Position:  sdk.Position("1"),
		// 			Operation: sdk.OperationCreate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "1",
		// 			},
		// 			Payload: sdk.Change{
		// 				After: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob",
		// 					"lastName":  "squarepants",
		// 				},
		// 			},
		// 		},
		// 	},
		// 	colOrder: []string{
		// 		"meroxa_operation", "meroxa_created_at", "meroxa_updated_at",
		// 		"meroxa_deleted_at", "id", "firstName", "lastName",
		// 	},
		// 	meroxaColumns: ConnectorColumns{
		// 		operationColumn: "meroxa_operation",
		// 		createdAtColumn: "meroxa_created_at",
		// 		updatedAtColumn: "meroxa_updated_at",
		// 		deletedAtColumn: "meroxa_deleted_at",
		// 	},
		// 	prefix:     "meroxa",
		// 	primaryKey: "id",
		// 	expectedBuffers: func() (*bytes.Buffer, *bytes.Buffer) {
		// 		insertBuf, updateBuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

		// 		insertWriter := csv.NewWriter(insertBuf)
		// 		err := insertWriter.Write([]string{
		// 			"create", fmt.Sprint(testTimestamp), "", "", "1", "spongebob", "squarepants",
		// 		})
		// 		require.NoError(t, err)
		// 		insertWriter.Flush()

		// 		return insertBuf, updateBuf
		// 	},
		// 	numGoRoutines: 1,
		// },
		// {
		// 	desc: "duplicate updates for same ID",
		// 	records: []sdk.Record{
		// 		{
		// 			Position:  sdk.Position("1"),
		// 			Operation: sdk.OperationUpdate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "1",
		// 			},
		// 			Payload: sdk.Change{
		// 				Before: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob",
		// 					"lastName":  "squarepants",
		// 				},
		// 				After: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob1",
		// 					"lastName":  "squarepants1",
		// 				},
		// 			},
		// 		},
		// 		{
		// 			Position:  sdk.Position("1"),
		// 			Operation: sdk.OperationUpdate,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp + 1),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "1",
		// 			},
		// 			Payload: sdk.Change{
		// 				Before: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob1",
		// 					"lastName":  "squarepants1",
		// 				},
		// 				After: sdk.StructuredData{
		// 					"id":        "1",
		// 					"firstName": "spongebob2",
		// 					"lastName":  "squarepants2",
		// 				},
		// 			},
		// 		},
		// 	},
		// 	colOrder: []string{
		// 		"meroxa_operation", "meroxa_created_at", "meroxa_updated_at",
		// 		"meroxa_deleted_at", "id", "firstName", "lastName",
		// 	},
		// 	meroxaColumns: ConnectorColumns{
		// 		operationColumn: "meroxa_operation",
		// 		createdAtColumn: "meroxa_created_at",
		// 		updatedAtColumn: "meroxa_updated_at",
		// 		deletedAtColumn: "meroxa_deleted_at",
		// 	},
		// 	prefix:     "meroxa",
		// 	primaryKey: "id",
		// 	expectedBuffers: func() (*bytes.Buffer, *bytes.Buffer) {
		// 		insertBuf, updateBuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

		// 		updateWriter := csv.NewWriter(updateBuf)
		// 		err := updateWriter.Write([]string{
		// 			"update", "", fmt.Sprint(testTimestamp + 1), "", "1", "spongebob2", "squarepants2",
		// 		})
		// 		require.NoError(t, err)
		// 		updateWriter.Flush()

		// 		return insertBuf, updateBuf
		// 	},
		// 	numGoRoutines: 1,
		// },
		// {
		// 	desc: "delete without payload.Before",
		// 	records: []sdk.Record{
		// 		{
		// 			Position:  sdk.Position("4"),
		// 			Operation: sdk.OperationDelete,
		// 			Metadata: sdk.Metadata{
		// 				"opencdc.readAt": fmt.Sprint(testTimestamp),
		// 			},
		// 			Key: sdk.StructuredData{
		// 				"id": "4",
		// 			},
		// 			Payload: sdk.Change{
		// 				Before: nil,
		// 			},
		// 		},
		// 	},
		// 	colOrder: []string{
		// 		"meroxa_operation", "meroxa_created_at", "meroxa_updated_at",
		// 		"meroxa_deleted_at", "id", "firstName", "lastName",
		// 	},
		// 	meroxaColumns: ConnectorColumns{
		// 		operationColumn: "meroxa_operation",
		// 		createdAtColumn: "meroxa_created_at",
		// 		updatedAtColumn: "meroxa_updated_at",
		// 		deletedAtColumn: "meroxa_deleted_at",
		// 	},
		// 	prefix:        "meroxa",
		// 	primaryKey:    "id",
		// 	numGoRoutines: 1,
		// 	expectedBuffers: func() (*bytes.Buffer, *bytes.Buffer) {
		// 		insertBuf, updateBuf := bytes.NewBuffer(nil), bytes.NewBuffer(nil)

		// 		updateWriter := csv.NewWriter(updateBuf)
		// 		err := updateWriter.Write([]string{
		// 			"delete", "", "", fmt.Sprint(testTimestamp), "4", "", "",
		// 		})
		// 		require.NoError(t, err)
		// 		updateWriter.Flush()

		// 		return insertBuf, updateBuf
		// 	},
		// },
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctx := context.Background()
			buf := bytes.NewBuffer(nil)

			err := MakeCSVBytes(
				ctx,
				tc.records,
				tc.table,
				buf,
			)

			if tc.expectedErr != nil {
				is.True(err != nil)
				is.Equal(err.Error(), tc.expectedErr.Error())
			} else {
				is.NoErr(err)
				t.Log(buf.String())
				t.Log(tc.wantCSV)
				is.Equal("", cmp.Diff(buf.String(), tc.wantCSV))
			}
		})
	}
}
