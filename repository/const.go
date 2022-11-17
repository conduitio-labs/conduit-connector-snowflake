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

	queryCreateStream        = `CREATE STREAM IF NOT EXISTS %s on table %s`
	queryCreateTrackingTable = `CREATE TABLE %s LIKE %s`
	queryAddTimestampColumn  = `ALTER TABLE %s ADD COLUMN %s TIMESTAMP`
	queryAddStringColumn     = `ALTER TABLE %s ADD COLUMN %s STRING`
	queryAddBooleanColumn    = `ALTER TABLE %s ADD COLUMN %s BOOLEAN`
	queryInsertInto          = `INSERT INTO %s %s`
	queryInsertIntoColumn    = `INSERT INTO %s (%s) %s`
	queryIsTableExist        = `SHOW TABLES LIKE '%s'`
	queryGetMaxValue         = `SELECT MAX(%s) FROM %s`
)
