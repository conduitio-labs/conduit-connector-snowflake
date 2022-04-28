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

import (
	"testing"
)

func Test_buildQuery(t *testing.T) {
	tests := []struct {
		name   string
		table  string
		fields []string
		offset int
		limit  int
		want   string
	}{
		{
			name:   "select all",
			table:  "test",
			fields: nil,
			offset: 0,
			limit:  100,
			want:   "SELECT * FROM test LIMIT 100 OFFSET 0",
		},
		{
			name:   "select fields",
			table:  "test",
			fields: []string{"id", "name"},
			offset: 0,
			limit:  100,
			want:   "SELECT id, name FROM test LIMIT 100 OFFSET 0",
		},
		{
			name:   "change offset",
			table:  "test",
			fields: []string{"id", "name"},
			offset: 1,
			limit:  100,
			want:   "SELECT id, name FROM test LIMIT 100 OFFSET 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildQuery(tt.table, tt.fields, tt.offset, tt.limit); got != tt.want {
				t.Errorf("buildQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
