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
	"fmt"
	"strings"
	"text/template"

	"github.com/conduitio-labs/conduit-connector-snowflake/destination/schema/snowflake"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type BatchType int

const (
	InsertBatch BatchType = iota + 1
	UpdateBatch
)

type Batch struct {
	ID       string
	Stage    string
	Type     BatchType
	Table    snowflake.Table
	Filename string

	Records []sdk.Record

	mergeQueryTemplate *template.Template
}

func NewInsertBatch(id, stage string, table snowflake.Table) *Batch {
	return &Batch{
		ID:       id,
		Stage:    stage,
		Type:     InsertBatch,
		Table:    table,
		Filename: fmt.Sprintf("%s_%s_inserts.csv.gz", id, table.Name),

		mergeQueryTemplate: insertBatchTemplate,
	}
}

func NewUpdateBatch(id, stage string, table snowflake.Table) *Batch {
	return &Batch{
		ID:       id,
		Stage:    stage,
		Type:     UpdateBatch,
		Table:    table,
		Filename: fmt.Sprintf("%s_%s_updates.csv.gz", id, table.Name),

		mergeQueryTemplate: updateBatchTemplate,
	}
}

func (b *Batch) SelectMerge() string {
	ret := make([]string, len(b.Table.Columns))
	for i, col := range b.Table.Columns {
		ret[i] = fmt.Sprintf("b.%s", col.Name)
	}

	return strings.Join(ret, ", ")
}

func (b *Batch) Columns(alias string) string {
	ret := make([]string, len(b.Table.Columns))
	for i, col := range b.Table.Columns {
		ret[i] = alias + "." + col.Name
	}
	return strings.Join(ret, ", ")

}

func (b *Batch) MergeQuery() (string, error) {
	var buf bytes.Buffer
	err := b.mergeQueryTemplate.Execute(&buf, b)
	if err != nil {
		return "", fmt.Errorf("failed to execute update merge query template: %w", err)
	}
	return buf.String(), nil
}

func (b *Batch) SetUpdate() string {
	return b.set(updateSetMode)
}

func (b *Batch) SetDelete() string {
	return b.set(deleteSetMode)
}

func (b *Batch) SetInsert() string {
	return b.set(insertSetMode)
}

type setListMode int

const (
	insertSetMode setListMode = iota + 1
	updateSetMode
	deleteSetMode
)

func (b *Batch) set(mode setListMode) string {
	const (
		alias1 = "a"
		alias2 = "b"
	)

	ret := make([]string, 0, len(b.Table.Columns))
	for _, col := range b.Table.Columns {
		// do not overwrite created_at on updates & deletes
		if col == b.Table.CreatedAt && (mode == updateSetMode || mode == deleteSetMode) {
			continue
		}
		// do not overwrite updated_at on deletes
		if col == b.Table.UpdatedAt && mode == deleteSetMode {
			continue
		}
		ret = append(ret, fmt.Sprintf("%s.%s = %s.%s", alias1, col.Name, alias2, col.Name))
	}

	return strings.Join(ret, ", ")
}

var insertBatchTemplate = template.Must(
	template.New("insertBatch").
		Parse(`
MERGE INTO "{{ .Table.Name }}" AS a
USING (
    SELECT {{ .SelectMerge }}
    FROM @{{ .Stage }}/{{ .Filename }} (FILE_FORMAT => CSV_CONDUIT_SNOWFLAKE)
) AS b
ON a.{{ .Table.PrimaryKey }} = b.{{ .Table.PrimaryKey }}
WHEN MATCHED AND (
    b.{{ .Table.Operation.Name }} = 'create'
    OR b.{{ .Table.Operation.Name }} = 'snapshot'
) THEN
    UPDATE
    SET {{ .SetInsert }}
WHEN NOT matched AND (
    b.{{ .Table.Operation.Name }} = 'create'
    OR b.{{ .Table.Operation.Name }} = 'snapshot'
) THEN
    INSERT ({{ .Columns "a" }})
    VALUES ({{ .Columns "b" }})
`),
)

var updateBatchTemplate = template.Must(
	template.New("updateBatch").
		Parse(`
MERGE INTO "{{ .Table.Name }}" AS a
USING (
    SELECT {{ .SelectMerge }}
    FROM @{{ .Stage }}/{{ .Filename }} (FILE_FORMAT => CSV_CONDUIT_SNOWFLAKE)
) AS b
ON a.{{ .Table.PrimaryKey }} = b.{{ .Table.PrimaryKey }}
WHEN MATCHED AND b.{{ .Table.Operation.Name }} = 'update'
THEN
    UPDATE
    SET {{ .SetUpdate }}
WHEN MATCHED AND b.{{ .Table.Operation.Name }} = 'delete'
THEN
    UPDATE
    SET {{ .SetDelete }}
WHEN NOT MATCHED AND (
    b.{{ .Table.Operation.Name }} = 'update'
    OR b.{{ .Table.Operation.Name }} = 'delete'
) THEN
    INSERT ({{ .Columns "a" }})
    VALUES ({{ .Columns "b" }})
`),
)
