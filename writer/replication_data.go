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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

type QuadUpdate struct {
	Start     int64
	Quads     []quad.Quad
	Action    graph.Procedure
	Timestamp time.Time
}

type ReplicaData struct {
	Address string
	addr    *url.URL
	Horizon int64
}

func (r *ReplicaData) sendToReplica(data []byte) error {
	switch r.addr.Scheme {
	case "http":
		outurl, err := r.addr.Parse("/api/v1/replication/write")
		if err != nil {
			return err
		}
		resp, err := http.Post(outurl.String(), "application/json", bytes.NewBuffer(data))
		if err != nil {
			// TODO(barakmich): More interesting error handling here
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("Adding quads failed!")
		}

	}
	return nil
}

type masterConnection struct {
	addr *url.URL
}

func (m masterConnection) sendToMaster(data []byte) error {
	switch m.addr.Scheme {
	case "http":
		outurl, err := m.addr.Parse("/api/v1/replication/write")
		if err != nil {
			return err
		}
		resp, err := http.Post(outurl.String(), "application/json", bytes.NewBuffer(data))
		if err != nil {
			// TODO(barakmich): More interesting error handling here
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("Adding quads failed!")
		}

	}
	return nil
}

type errorMsg struct {
	Err string `json:"error"`
}

func (m masterConnection) registerMaster(rep *Replica, url *url.URL) error {
	repData := &ReplicaData{
		Address: url.String(),
		Horizon: rep.currentID.Int(),
	}
	fmt.Println("Got URL", m.addr)
	data, err := json.Marshal(repData)
	if err != nil {
		return err
	}
	outurl, err := m.addr.Parse("/api/v1/replication/register")
	if err != nil {
		return err
	}
	resp, err := http.Post(outurl.String(), "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		var errmsg errorMsg
		defer resp.Body.Close()
		enc := json.NewDecoder(resp.Body)
		enc.Decode(&errmsg)
		glog.Errorf("error registering: %s", errmsg.Err)
		return fmt.Errorf("%s", errmsg.Err)
	}
	return nil

}
