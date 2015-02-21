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

const (
	defaultReplicaPageSize = 10000
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
	pageSize        int64
}

func NewReplicaReplication(qs graph.QuadStore, opts graph.Options) (graph.QuadWriter, error) {
	ignoreOpts := fillIgnoreOpts(opts)
	r := &Replica{
		currentID:  qs.Horizon(),
		qs:         qs,
		ignoreOpts: ignoreOpts,
	}

	r.pageSize = defaultReplicaPageSize
	if val, ok := opts.IntKey("replica_page_size"); ok {
		r.pageSize = int64(val)
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
	rep.master.ourURL = url
	err := rep.master.registerMaster(rep, "connect")
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
		if glog.V(4) {
			glog.V(4).Infof("writeUpdate at %d", up.Start)
		}
		if rep.currentID.Int() == up.Start {
			glog.V(2).Infof("Writing replica write from %d to %d", up.Start, up.Start+int64(len(up.Deltas)))
			rep.localRepWrite(up)
			progress = true
			rep.incomingUpdates = append(rep.incomingUpdates[:i], rep.incomingUpdates[i+1:]...)
		} else if rep.currentID.Int() > up.Start {
			glog.Infoln("removing apparently stale data starting at", up.Start)
			rep.incomingUpdates = append(rep.incomingUpdates[:i], rep.incomingUpdates[i+1:]...)
		}
	}
	if !progress {
		glog.Infof("replica-write: Couldn't find a useful update in %d updates. Current horizon: %d", len(rep.incomingUpdates), rep.currentID.Int())
		return
	}
	if len(rep.incomingUpdates) != 0 && progress {
		go rep.writeUpdates()
	}
}

func (rep *Replica) localRepWrite(up QuadUpdate) {
	rep.qs.ApplyDeltas(up.Deltas, rep.ignoreOpts)
}

func (rep *Replica) masterApply(quads []quad.Quad, action graph.Procedure) error {
	update := ReplicaWrite{
		Quads:  quads,
		Action: action,
	}
	bytes, err := json.Marshal(update)
	if err != nil {
		return err
	}
	return rep.master.sendWriteToMaster(bytes)
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
	err := rep.master.registerMaster(rep, "close")
	if err != nil {
		return err
	}
	return nil
}

func (rep *Replica) catchUp(mhorizon int64) {
	rep.lock.Lock()
	currentID := rep.currentID.Int()
	rep.lock.Unlock()
	if mhorizon < currentID {
		glog.Fatalf("Trying to connect as a replica to a master which is behind us. MasterHorizon %d, LocalHorizon %d", mhorizon, currentID)
	}
	if mhorizon == currentID {
		return
	}
	if mhorizon > currentID {
		dec, err := rep.master.sendCommandToMaster(
			"/api/v1/replication/catchup",
			CatchUpMsg{
				From: currentID,
				Size: rep.pageSize,
			})
		if err != nil {
			//TODO(barakmich): Drop the connection and retry.
			glog.Fatalln(err)
		}
		var up QuadUpdate
		dec.Decode(&up)
		rep.localRepWrite(up)
		if currentID+defaultReplicaPageSize < mhorizon {
			go rep.catchUp(mhorizon)
		}
	}
}
