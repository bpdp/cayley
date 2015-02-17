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

	"github.com/barakmich/glog"
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
	incomingUpdates []QuadUpdate
	master          masterConnection
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
		r.master.addr = url
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

func (rep *Replica) RegisterHTTP(r *httprouter.Router, url *url.URL) {
	if rep.listenAddr == nil {
		rep.registerHTTP(r, url)
	}
}

func (rep *Replica) registerHTTP(r *httprouter.Router, url *url.URL) {
	r.POST("/api/v1/replication/write", rep.replicaWrite)
	err := rep.master.registerMaster(rep, url)
	if err != nil {
		glog.Fatalln("replica:", err, rep, url)
	}
}

func (rep *Replica) replicaWrite(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
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
	rep.lock.Lock()
	rep.incomingUpdates = append(rep.incomingUpdates, update)
	rep.lock.Unlock()
	go rep.writeUpdates()
	w.WriteHeader(200)
}

func (rep *Replica) writeUpdates() {
	rep.lock.Lock()
	defer rep.lock.Unlock()
	if len(rep.incomingUpdates) == 0 {
		return
	}
	progress := false
	for i, up := range rep.incomingUpdates {
		// The error occured on the master. This will go through without issue.
		if rep.currentID.Int() == up.Start {
			rep.localRepWrite(up)
			progress = true
			rep.incomingUpdates = append(rep.incomingUpdates[:i], rep.incomingUpdates[i:]...)
		}
	}
	if !progress {
		glog.Infof("replica-write: Couldn't find a useful update in %d updates. Current horizon: %d", len(rep.incomingUpdates), rep.currentID.Int())
	}
	if len(rep.incomingUpdates) != 0 && progress {
		go rep.writeUpdates()
	}
}

func (rep *Replica) localRepWrite(up QuadUpdate) {
	deltas := make([]graph.Delta, len(up.Quads))
	for i, q := range up.Quads {
		deltas[i] = graph.Delta{
			ID:        rep.currentID.Next(),
			Quad:      q,
			Action:    up.Action,
			Timestamp: up.Timestamp,
		}
	}

	rep.qs.ApplyDeltas(deltas, rep.ignoreOpts)
}

func (rep *Replica) masterApply(quads []quad.Quad, action graph.Procedure) error {
	update := QuadUpdate{
		Quads:  quads,
		Action: action,
	}
	bytes, err := json.Marshal(update)
	if err != nil {
		return err
	}
	return rep.master.sendToMaster(bytes)
}

func (rep *Replica) AddQuad(q quad.Quad) error {
	return rep.masterApply([]quad.Quad{q}, graph.Add)
}

func (rep *Replica) AddQuadSet(set []quad.Quad) error {
	return rep.masterApply(set, graph.Add)
}

func (rep *Replica) RemoveQuad(q quad.Quad) error {
	return rep.masterApply([]quad.Quad{q}, graph.Delete)
}

func (rep *Replica) Close() error {
	return nil
}
