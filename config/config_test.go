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

package config

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		cfg         map[string]string
		want        Config
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid config",
			cfg: map[string]string{
				KeyConnection:     "user:password@my_organization-my_account/mydb/public",
				KeyTable:          "customer",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "100",
				KeyOrderingColumn: "id",
			},
			want: Config{
				Connection:     "user:password@my_organization-my_account/mydb/public",
				Table:          "customer",
				Key:            "ID",
				BatchSize:      100,
				OrderingColumn: "ID",
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "default batchSize",
			cfg: map[string]string{
				KeyConnection:     "user:password@my_organization-my_account/mydb/public",
				KeyTable:          "customer",
				KeyPrimaryKey:     "id",
				KeyOrderingColumn: "id",
			},
			want: Config{
				Connection:     "user:password@my_organization-my_account/mydb/public",
				Table:          "customer",
				Key:            "ID",
				BatchSize:      defaultBatchSize,
				OrderingColumn: "ID",
			},
			wantErr:     false,
			expectedErr: "",
		},
		{
			name: "missing connection field",
			cfg: map[string]string{
				KeyTable:          "customer",
				KeyColumns:        "",
				KeyPrimaryKey:     "id",
				KeyOrderingColumn: "id",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: `validate config: Connection value must be set`,
		},
		{
			name: "missing table field",
			cfg: map[string]string{
				KeyConnection:     "user:password@my_organization-my_account/mydb",
				KeyColumns:        "",
				KeyPrimaryKey:     "id",
				KeyOrderingColumn: "id",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: `validate config: Table value must be set`,
		},
		{
			name: "missing key field",
			cfg: map[string]string{
				KeyConnection:     "user:password@my_organization-my_account/mydb",
				KeyTable:          "customer",
				KeyColumns:        "",
				KeyOrderingColumn: "id",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: `validate config: Key value must be set`,
		},
		{
			name: "missing orderingColumn field",
			cfg: map[string]string{
				KeyConnection: "user:password@my_organization-my_account/mydb",
				KeyTable:      "customer",
				KeyColumns:    "",
				KeyPrimaryKey: "id",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: `validate config: orderingColumn value must be set`,
		},
		{
			name: "too long table name",
			cfg: map[string]string{
				KeyConnection: "user:password@my_organization-my_account/mydb",
				KeyTable: "some_specific_really_big_name_with_additional_not_needed_things_" +
					"really_big_description_some_specific_really_big_name_with" +
					"_additional_not_needed_things_some_specific_really_big_name_with_additional_not_needed_things_" +
					"some_specific_really_big_name_with_additional_not_needed_things_" +
					"_additional_not_needed_things_some_specific_really_big_name_with_additional_not_needed_things",
				KeyColumns:        "",
				KeyPrimaryKey:     "id",
				KeyOrderingColumn: "id",
			},
			want:        Config{},
			wantErr:     true,
			expectedErr: `validate config: "Table" value must be less than or equal to 255`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.cfg)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr, err.Error())

					return
				}

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}
