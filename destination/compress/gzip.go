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

package compress

import (
	"compress/gzip"
	"io"

	"github.com/go-errors/errors"
)

var _ Compressor = (*Gzip)(nil)

type Gzip struct{}

func (Gzip) Compress(in io.Reader, out io.Writer) error {
	w := gzip.NewWriter(out)
	if _, err := io.Copy(w, in); err != nil {
		return errors.Errorf("failed to copy bytes to gzip writer: %w", err)
	}

	if err := w.Close(); err != nil {
		return errors.Errorf("failed to flush gzip writer: %w", err)
	}

	return nil
}

func (Gzip) Name() string {
	return TypeGzip
}
