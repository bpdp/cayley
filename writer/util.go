// Copyright 2015 The Cayley Authors. All rights reserved.
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
	"github.com/google/cayley/graph"
)

func fillIgnoreOpts(opts graph.Options) graph.IgnoreOpts {
	var ignoreMissing, ignoreDuplicate bool

	if *graph.IgnoreMissing {
		ignoreMissing = true
	} else {
		ignoreMissing, _ = opts.BoolKey("ignore_missing")
	}

	if *graph.IgnoreDup {
		ignoreDuplicate = true
	} else {
		ignoreDuplicate, _ = opts.BoolKey("ignore_duplicate")
	}
	return graph.IgnoreOpts{
		IgnoreDup:     ignoreDuplicate,
		IgnoreMissing: ignoreMissing,
	}
}
