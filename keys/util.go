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

package keys

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/cayley/graph"
)

func MakePrimaryKey(s string) (graph.PrimaryKey, error) {
	parts := strings.Split(s, ":")
	switch parts[0] {
	case "sequential":
		parse, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, err
		}
		return NewSequentialKey(parse), nil

	}
	return nil, fmt.Errorf("keys: Unknown PrimaryKey type %s", s)
}
