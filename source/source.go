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

package source

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-snowflake/config"
	"github.com/conduitio-labs/conduit-connector-snowflake/source/iterator"
)

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	sourceConfig SourceConfig
	iterator     Iterator
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyConnection: {
			Default:     "",
			Required:    true,
			Description: "Snowflake connection string.",
		},
		config.KeyTable: {
			Default:     "",
			Required:    true,
			Description: "The table name that the connector should read.",
		},
		config.KeyOrderingColumn: {
			Default:  "",
			Required: true,
			Description: "The name of a column that the connector will use for ordering rows. " +
				"Its values must be unique and suitable for sorting, otherwise, the snapshot won't work correctly.",
		},
		config.KeyColumns: {
			Default:     "",
			Required:    false,
			Description: "Comma separated list of column names that should be included in each Record's payload.",
		},
		config.KeyPrimaryKeys: {
			Default:     "",
			Required:    false,
			Description: "The list of the column names that records should use for their `Key` fields.",
		},
		config.KeySnapshot: {
			Default:  "true",
			Required: false,
			Description: "Whether or not the plugin will take a snapshot of the entire table before starting cdc " +
				"mode, by default true.",
		},
		config.KeyBatchSize: {
			Default:     "1000",
			Required:    false,
			Description: "Size of batch",
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	cfg, err := config.Parse(cfgRaw)
	if err != nil {
		return err
	}

	// parse source batch size and snapshot manually, set defaults
	s.sourceConfig.BatchSize = 1000
	s.sourceConfig.Snapshot = true
	s.sourceConfig.Config = cfg

	if cfgRaw[config.KeyBatchSize] != "" {
		batchSize, err := strconv.Atoi(cfgRaw[config.KeyBatchSize])
		if err != nil {
			return errors.New(`"batchSize" config value must be int`)
		}

		s.sourceConfig.BatchSize = batchSize
	}

	if cfgRaw[config.KeySnapshot] != "" {
		snapshot, err := strconv.ParseBool(cfgRaw[config.KeySnapshot])
		if err != nil {
			return fmt.Errorf("parse %q: %w", config.KeySnapshot, err)
		}

		s.sourceConfig.Snapshot = snapshot
	}

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	it, err := iterator.New(ctx, s.sourceConfig.Connection, s.sourceConfig.Table, s.sourceConfig.OrderingColumn, s.sourceConfig.Keys,
		s.sourceConfig.Columns, s.sourceConfig.BatchSize, s.sourceConfig.Snapshot, rp)
	if err != nil {
		return fmt.Errorf("create iterator: %w", err)
	}

	s.iterator = it

	return nil
}

// Read gets the next object from the snowflake.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("next: %w", err)
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (s *Source) Ack(ctx context.Context, p sdk.Position) error {
	return s.iterator.Ack(ctx, p)
}
