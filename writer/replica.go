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
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/julienschmidt/httprouter"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

func init() {
	graph.RegisterWriter("replica", NewReplicaReplication)
}

type Replica struct {
	currentID       graph.PrimaryKey
	qs              graph.QuadStore
	ignoreOpts      graph.IgnoreOpts
	lock            sync.Mutex
	incomingUpdates []ReplicaUpdate
	master          masterConnection
	masterAddr      *url.URL
	listenAddr      *url.URL
}

func NewReplicaReplication(qs graph.QuadStore, opts graph.Options) (graph.QuadWriter, error) {
	ignoreOpts := fillIgnoreOpts(opts)
	r := &Replica{
		currentID:  qs.Horizon(),
		qs:         qs,
		ignoreOpts: ignoreOpts,
	}

	if addr, ok := opts.StringKey("master_url"); ok {
		if addr == "" {
			return nil, fmt.Errorf("Empty `master_url` passed in configuration to a replica.")
		}
		url, err := url.Parse(addr)
		if err != nil {
			return nil, err
		}
		r.masterAddr = url
	} else {
		return nil, fmt.Errorf("No `master_url` passed in configuration to a replica.")
	}

	if addr, ok := opts.StringKey("listen_url"); ok {
		if addr != "" {
			url, err := url.Parse(addr)
			if err != nil {
				return nil, err
			}
			r.listenAddr = url
			r.makeAndRunListener()
		}
	}
	return r, nil
}

func (rep *Replica) makeAndRunListener() {
	// Make endpoint.
	// TODO(barakmich): Potential for non-http replication endpoints.
	// Seems *very* likely.
	// Register functions.
	// Run Listener.
	go func() {
	}()
}

func (rep *Replica) RegisterHTTP(r *httprouter.Router) {
	if rep.listenAddr == nil {
		rep.registerHTTP(r)
	}
}

func (rep *Replica) registerHTTP(r *httprouter.Router) {
	r.POST("/api/v1/replication/write", rep.replicaWrite)
}

func (rep *Replica) replicaWrite(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
}

func (rep *Replica) masterApplyDeltas(d []graph.Delta) error {
	return nil
}

func (rep *Replica) AddQuad(q quad.Quad) error {
	deltas := make([]graph.Delta, 1)
	deltas[0] = graph.Delta{
		Quad:   q,
		Action: graph.Add,
	}
	return rep.masterApplyDeltas(deltas)
}

func (rep *Replica) AddQuadSet(set []quad.Quad) error {
	deltas := make([]graph.Delta, len(set))
	for i, q := range set {
		deltas[i] = graph.Delta{
			Quad:   q,
			Action: graph.Add,
		}
	}

	return rep.masterApplyDeltas(deltas)
}

func (rep *Replica) RemoveQuad(q quad.Quad) error {
	deltas := make([]graph.Delta, 1)
	deltas[0] = graph.Delta{
		Quad:   q,
		Action: graph.Delete,
	}
	return rep.masterApplyDeltas(deltas)
}

func (rep *Replica) Close() error {
	return nil
}
