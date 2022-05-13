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

package snapshot

import (
	"fmt"
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio/conduit-connector-snowflake/source/position"
)

func TestParseSDKPosition(t *testing.T) {
	tests := []struct {
		name        string
		in          sdk.Position
		want        Position
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid sdk position",
			in:   sdk.Position("s.20.1"),
			want: Position{
				Type:    position.TypeSnapshot,
				Element: 20,
				Offset:  1,
			},
		},
		{
			name:    "wrong the number of position elements",
			in:      sdk.Position("s.20"),
			wantErr: true,
			expectedErr: fmt.Sprintf("the number of position elements must be equal to %d, now it is 2",
				reflect.TypeOf(Position{}).NumField()),
		},
		{
			name:        "wrong element type",
			in:          sdk.Position("s.test.3"),
			wantErr:     true,
			expectedErr: position.ErrFieldInvalidElement.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSDKPosition(tt.in)
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

func TestFormatSDKPosition(t *testing.T) {
	tests := []struct {
		name       string
		pos        Position
		wantSDKPos sdk.Position
	}{
		{
			name: "sdk position",
			pos: Position{
				Type:    position.TypeSnapshot,
				Element: 20,
				Offset:  1,
			},
			wantSDKPos: sdk.Position("s.20.1"),
		},
		{
			name: "sdk position",
			pos: Position{
				Type:    position.TypeSnapshot,
				Element: 35,
				Offset:  10,
			},
			wantSDKPos: sdk.Position("s.35.10"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.pos.FormatSDKPosition()
			if !reflect.DeepEqual(got, tt.wantSDKPos) {
				t.Errorf("parse = %v, want %v", got, tt.wantSDKPos)
			}
		})
	}
}
