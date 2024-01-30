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
	queryCreateTable          = `CREATE TABLE IF NOT EXISTS %s (%s, meroxa_deleted_at TIMESTAMP_LTZ, meroxa_created_at TIMESTAMP_LTZ, meroxa_updated_at TIMESTAMP_LTZ )`
	queryCreateTemporaryTable = `CREATE TEMPORARY TABLE IF NOT EXISTS %s (%s)`
	queryCreateStage          = `CREATE STAGE IF NOT EXISTS %s`
	queryAddTimestampColumn   = `ALTER TABLE %s ADD COLUMN %s TIMESTAMP`
	queryAddStringColumn      = `ALTER TABLE %s ADD COLUMN %s STRING`
	queryAddBooleanColumn     = `ALTER TABLE %s ADD COLUMN %s BOOLEAN`
	queryInsertInto           = `INSERT INTO %s %s`
	queryInsertIntoColumn     = `INSERT INTO %s (%s) %s`
	queryIsTableExist         = `SHOW TABLES LIKE '%s'`
	queryGetMaxValue          = `SELECT MAX(%s) FROM %s`
	queryGetPrimaryKeys       = `SHOW PRIMARY KEYS IN TABLE %s`
	queryPutFileInStage       = `PUT file://%s @%s parallel=30;` // TODO: make parallelism configurable.
	queryCopyInto             = `COPY INTO %s FROM @%s pattern='%s.gz' FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ','  PARSE_HEADER = TRUE) MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE' ;`
	queryMergeInto            = `MERGE INTO %s as a USING %s AS b ON %s
		WHEN MATCHED AND b.%s_operation = 'update' THEN UPDATE SET %s, a.%s_updated_at = CURRENT_TIMESTAMP()
	    WHEN MATCHED AND b.%s_operation = 'delete' THEN UPDATE SET a.%s_deleted_at = CURRENT_TIMESTAMP()
		WHEN NOT MATCHED THEN INSERT (%s , a.%s_created_at) VALUES (%s, CURRENT_TIMESTAMP())`
	queryDropTable  = `DROP table %s`
	queryRemoveFile = `REMOVE @%s/%s`

	columnName = "column_name"
)
