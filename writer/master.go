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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/keys"
	"github.com/google/cayley/quad"
)

func init() {
	graph.RegisterWriter("master", NewMasterReplication)
}

type Master struct {
	currentID  graph.PrimaryKey
	qs         graph.QuadStore
	ignoreOpts graph.IgnoreOpts
	lock       sync.Mutex
	replicas   []ReplicaData
}

type ReplicaData struct {
	Protocol       string
	Address        string
	InitialHorizon string
	horizon        graph.PrimaryKey
}

func NewMasterReplication(qs graph.QuadStore, opts graph.Options) (graph.QuadWriter, error) {
	ignoreOpts := fillIgnoreOpts(opts)
	m := &Master{
		currentID:  qs.Horizon(),
		qs:         qs,
		ignoreOpts: ignoreOpts,
	}
	if addr, ok := opts.StringKey("listen_address"); ok {
		if addr != "" {
			m.makeAndRunListener(addr)
		}
	}
	return m, nil
}

func (m *Master) makeAndRunListener(addr string) {
	// Make endpoint.
	// TODO(barakmich): Potential for non-http registration endpoints.
	// Seems unlikely, as this is a one-time registration.
	// Register functions.
	// Run Listener.
	go func() {
	}()
}

func (m *Master) RegisterHTTP(r *httprouter.Router) {
	r.POST("/api/v1/replication/register", m.registerReplica)
}

func (m *Master) registerReplica(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", err), 400)
		return
	}
	var newReplica ReplicaData
	err = json.Unmarshal(bodyBytes, &newReplica)
	if err != nil {
		http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", err), 400)
		return
	}
	newReplica.horizon, err = keys.MakePrimaryKey(newReplica.InitialHorizon)
	if err != nil {
		http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", err), 400)
		return
	}
	m.lock.Lock()
	m.replicas = append(m.replicas, newReplica)
	m.lock.Unlock()
	if newReplica.horizon.Int() < m.currentID.Int() {
		newReplica.updateReplica(newReplica.horizon, m.currentID)
	}
}

func (m *Master) AddQuad(q quad.Quad) error {
	deltas := make([]graph.Delta, 1)
	deltas[0] = graph.Delta{
		ID:        m.currentID.Next(),
		Quad:      q,
		Action:    graph.Add,
		Timestamp: time.Now(),
	}
	return m.qs.ApplyDeltas(deltas, m.ignoreOpts)
}

func (m *Master) AddQuadSet(set []quad.Quad) error {
	deltas := make([]graph.Delta, len(set))
	for i, q := range set {
		deltas[i] = graph.Delta{
			ID:        m.currentID.Next(),
			Quad:      q,
			Action:    graph.Add,
			Timestamp: time.Now(),
		}
	}

	return m.qs.ApplyDeltas(deltas, m.ignoreOpts)
}

func (m *Master) RemoveQuad(q quad.Quad) error {
	deltas := make([]graph.Delta, 1)
	deltas[0] = graph.Delta{
		ID:        m.currentID.Next(),
		Quad:      q,
		Action:    graph.Delete,
		Timestamp: time.Now(),
	}
	return m.qs.ApplyDeltas(deltas, m.ignoreOpts)
}

func (m *Master) updateAllReplicas(set []quad.Quad) {

}

func (m *Master) Close() error {
	return nil
}

func (r *ReplicaData) updateReplica(from, to graph.PrimaryKey) {
}
