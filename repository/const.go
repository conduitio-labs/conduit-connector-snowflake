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

package repository

const (
	MetadataColumnAction = "METADATA$ACTION"
	MetadataColumnUpdate = "METADATA$ISUPDATE"
	MetadataColumnRow    = "METADATA$ROW_ID"
	MetadataColumnTime   = "METADATA$TS"

	queryCreateStream         = `CREATE STREAM IF NOT EXISTS %s on table %s`
	queryCreateTrackingTable  = `CREATE TABLE IF NOT EXISTS %s LIKE %s`
	queryCreateTemporaryTable = `CREATE TEMPORARY TABLE IF NOT EXISTS %s LIKE %s`
	queryCreateStage          = `CREATE STAGE IF NOT EXISTS %s`
	queryAddTimestampColumn   = `ALTER TABLE %s ADD COLUMN %s TIMESTAMP`
	queryAddStringColumn      = `ALTER TABLE %s ADD COLUMN %s STRING`
	queryAddBooleanColumn     = `ALTER TABLE %s ADD COLUMN %s BOOLEAN`
	queryInsertInto           = `INSERT INTO %s %s`
	queryInsertIntoColumn     = `INSERT INTO %s (%s) %s`
	queryIsTableExist         = `SHOW TABLES LIKE '%s'`
	queryGetMaxValue          = `SELECT MAX(%s) FROM %s`
	queryGetPrimaryKeys       = `SHOW PRIMARY KEYS IN TABLE %s`
	queryPutFileInStage       = `PUT %s %s;`
	queryCopyInto             = `COPY INTO %s FROM %s %s`
	// TODO: support DELETE & read operation from column
	queryMergeInto = `MERGE INTO %s as a USING %s AS b ON a.id = b.id
		WHEN MATCHED THEN UPDATE SET %s
		WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)`
	queryDropTable  = `DROP table %s`
	queryRemoveFile = `REMOVE @%s/%s`

	columnName = "column_name"
)
