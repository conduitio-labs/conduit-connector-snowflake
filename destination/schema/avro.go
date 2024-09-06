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

package schema

import (
	"context"

	//"github.com/hamba/avro/v2"
	//"github.com/go-errors/errors"
	//"github.com/conduitio/conduit-commons/schema"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	sdkschema "github.com/conduitio/conduit-connector-sdk/schema"
)

func ExtractSchema(ctx context.Context, r opencdc.Record) error {
	subject, err := r.Metadata.GetPayloadSchemaSubject()
	if err != nil {
		return err
	}

	ver, err := r.Metadata.GetPayloadSchemaVersion()
	if err != nil {
		return err
	}

	sch, err := sdkschema.Get(ctx, subject, ver)
	if err != nil {
		return err
	}

	sdk.Logger(ctx).Info().
		Str("subject", subject).
		Int("version", ver).
		Int("id", sch.ID).
		Str("type", sch.Type.String()).
		Str("data", string(sch.Bytes)).
		Msg("schema parsed and downloaded")

	return nil
}
