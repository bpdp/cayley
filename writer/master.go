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
	"net/url"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/google/cayley/graph"
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
	listenAddr *url.URL
}

func NewMasterReplication(qs graph.QuadStore, opts graph.Options) (graph.QuadWriter, error) {
	ignoreOpts := fillIgnoreOpts(opts)
	m := &Master{
		currentID:  qs.Horizon(),
		qs:         qs,
		ignoreOpts: ignoreOpts,
	}
	if addr, ok := opts.StringKey("listen_url"); ok {
		if addr != "" {
			url, err := url.Parse(addr)
			if err != nil {
				return nil, err
			}
			m.listenAddr = url
			m.makeAndRunListener()
		}
	}
	return m, nil
}

func (m *Master) makeAndRunListener() {
	if m.listenAddr == nil {
		panic("Undefined listenAddr")
	}
	// Make endpoint.
	// TODO(barakmich): Potential for non-http registration endpoints.
	// Seems unlikely, as this is a one-time registration.
	// Register functions.
	// Run Listener.
	go func() {
	}()
}

func (m *Master) RegisterHTTP(r *httprouter.Router) {
	if m.listenAddr == nil {
		m.registerHTTP(r)
	}
}

func (m *Master) registerHTTP(r *httprouter.Router) {
	r.POST("/api/v1/replication/register", m.registerReplica)
	r.POST("/api/v1/replication/write", m.writeFromReplica)
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
	m.lock.Lock()
	m.replicas = append(m.replicas, newReplica)
	m.lock.Unlock()
	if newReplica.Horizon < m.currentID.Int() {
		newReplica.updateReplica(newReplica.Horizon, m.currentID.Int())
	}
}

func (m *Master) writeFromReplica(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	bodyBytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", err), 400)
		return
	}
	var update QuadUpdate
	err = json.Unmarshal(bodyBytes, &update)
	if err != nil {
		http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", err), 400)
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	if update.Action == graph.Add {
		err := m.AddQuadSet(update.Quads)
		if err != nil {
			http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", err), 400)
			return
		}
	} else if update.Action == graph.Delete {
		for _, q := range update.Quads {
			err := m.RemoveQuad(q)
			if err != nil {
				http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", err), 400)
				return
			}
		}
	}
	http.Error(w, fmt.Sprintf("{\"error\" : \"%s\"}", "Invalid action?"), 400)
	return
}

func (m *Master) AddQuad(q quad.Quad) error {
	start := m.currentID
	deltas := make([]graph.Delta, 1)
	timestamp := time.Now()
	deltas[0] = graph.Delta{
		ID:        m.currentID.Next(),
		Quad:      q,
		Action:    graph.Add,
		Timestamp: timestamp,
	}
	err := m.qs.ApplyDeltas(deltas, m.ignoreOpts)
	if err != nil {
		return err
	}
	update := QuadUpdate{
		Start:     start.Int(),
		Quads:     []quad.Quad{q},
		Action:    graph.Add,
		Timestamp: timestamp,
	}
	return m.updateAllReplicas(update)
}

func (m *Master) AddQuadSet(set []quad.Quad) error {
	start := m.currentID
	deltas := make([]graph.Delta, len(set))
	timestamp := time.Now()
	for i, q := range set {
		deltas[i] = graph.Delta{
			ID:        m.currentID.Next(),
			Quad:      q,
			Action:    graph.Add,
			Timestamp: timestamp,
		}
	}

	err := m.qs.ApplyDeltas(deltas, m.ignoreOpts)
	if err != nil {
		return err
	}
	update := QuadUpdate{
		Start:     start.Int(),
		Quads:     set,
		Action:    graph.Add,
		Timestamp: timestamp,
	}
	return m.updateAllReplicas(update)
}

func (m *Master) RemoveQuad(q quad.Quad) error {
	start := m.currentID
	deltas := make([]graph.Delta, 1)
	timestamp := time.Now()
	deltas[0] = graph.Delta{
		ID:        m.currentID.Next(),
		Quad:      q,
		Action:    graph.Delete,
		Timestamp: timestamp,
	}
	err := m.qs.ApplyDeltas(deltas, m.ignoreOpts)
	if err != nil {
		return err
	}
	update := QuadUpdate{
		Start:     start.Int(),
		Quads:     []quad.Quad{q},
		Action:    graph.Delete,
		Timestamp: timestamp,
	}
	return m.updateAllReplicas(update)
}

func (m *Master) updateAllReplicas(update QuadUpdate) error {
	bytes, err := json.Marshal(update)
	if err != nil {
		return err
	}
	for _, l := range m.replicas {
		l.sendToReplica(bytes)
	}
	return nil
}

func (m *Master) Close() error {
	return nil
}

func (r *ReplicaData) updateReplica(from, to int64) {
}
