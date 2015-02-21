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

	"github.com/barakmich/glog"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/quad"
)

type ReplicaWrite struct {
	Quads  []quad.Quad
	Action graph.Procedure
}

type QuadUpdate struct {
	Start  int64
	Deltas []graph.Delta
}

type ReplicaData struct {
	Address string
	addr    *url.URL
	Horizon int64
	Status  string
}

type MasterData struct {
	Horizon int64
}

type CatchUpMsg struct {
	From int64
	Size int64
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
	addr   *url.URL
	ourURL *url.URL
}

func (m masterConnection) sendWriteToMaster(data []byte) error {
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

func (m masterConnection) sendCommandToMaster(api string, msg interface{}, out interface{}) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	outurl, err := m.addr.Parse(api)
	if err != nil {
		return err
	}
	resp, err := http.Post(outurl.String(), "application/json", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	if resp.StatusCode != 200 {
		var errmsg errorMsg
		dec.Decode(&errmsg)
		return fmt.Errorf("%s", errmsg.Err)
	}
	err = dec.Decode(out)
	if err != nil {
		return err
	}
	return nil
}

func (m masterConnection) registerMaster(rep *Replica, status string) error {
	repData := &ReplicaData{
		Address: m.ourURL.String(),
		Horizon: rep.currentID.Int(),
		Status:  status,
	}
	var mdata MasterData
	err := m.sendCommandToMaster("/api/v1/replication/register", repData, &mdata)
	if err != nil {
		glog.Errorf("error registering: %s", err.Error())
		return err
	}
	go rep.catchUp(mdata.Horizon)
	return nil

}
