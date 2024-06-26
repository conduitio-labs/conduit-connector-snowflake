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
)

var _ Compressor = (*Gzip)(nil)

type Gzip struct{}

func (Gzip) Compress(in io.Reader) io.Reader {
	pr, pw := io.Pipe()
	w := gzip.NewWriter(pw)

	go func() {
		defer pw.Close()
		defer w.Close()

		_, err := io.Copy(w, in)
		pw.CloseWithError(err)
	}()

	return pr
}

func (Gzip) Name() string {
	return TypeGzip
}
