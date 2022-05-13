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

package position

import (
	"strings"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Type describe position type.
type Type string

const (
	TypeSnapshot = "s"
	TypeCDC      = "c"
)

// GetType get position type.
func GetType(p sdk.Position) (Type, error) {
	parts := strings.Split(string(p), ".")

	if parts[0] == TypeSnapshot {
		return TypeSnapshot, nil
	}

	if parts[0] == TypeCDC {
		return TypeCDC, nil
	}

	return "", ErrInvalidType
}
