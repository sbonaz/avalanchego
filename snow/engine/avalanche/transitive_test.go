// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avalanche

import (
	"bytes"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ava-labs/avalanche-go/ids"
	"github.com/ava-labs/avalanche-go/snow/choices"
	"github.com/ava-labs/avalanche-go/snow/consensus/avalanche"
	"github.com/ava-labs/avalanche-go/snow/consensus/snowball"
	"github.com/ava-labs/avalanche-go/snow/consensus/snowstorm"
	"github.com/ava-labs/avalanche-go/snow/engine/avalanche/vertex"
	"github.com/ava-labs/avalanche-go/snow/engine/common"
	"github.com/ava-labs/avalanche-go/snow/validators"
)

var (
	errUnknownVertex = errors.New("unknown vertex")
	errFailedParsing = errors.New("failed parsing")
	errMissing       = errors.New("missing")
)

func TestEngineShutdown(t *testing.T) {
	config := DefaultConfig()
	vmShutdownCalled := false
	vm := &vertex.TestVM{}
	vm.ShutdownF = func() error { vmShutdownCalled = true; return nil }
	config.VM = vm

	transitive := &Transitive{}

	transitive.Initialize(config)
	transitive.finishBootstrapping()
	transitive.Ctx.Bootstrapped()
	transitive.Shutdown()
	if !vmShutdownCalled {
		t.Fatal("Shutting down the Transitive did not shutdown the VM")
	}
}

func TestEngineAdd(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	manager.CantEdge = false

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	if !te.Ctx.ChainID.Equals(ids.Empty) {
		t.Fatalf("Wrong chain ID")
	}

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{
			&avalanche.TestVertex{TestDecidable: choices.TestDecidable{
				IDV:     ids.GenerateTestID(),
				StatusV: choices.Unknown,
			}},
		},
		BytesV: []byte{1},
	}

	asked := new(bool)
	reqID := new(uint32)
	sender.GetF = func(inVdr ids.ShortID, requestID uint32, vtxID ids.ID) {
		*reqID = requestID
		if *asked {
			t.Fatalf("Asked multiple times")
		}
		*asked = true
		if !vdr.Equals(inVdr) {
			t.Fatalf("Asking wrong validator for vertex")
		}
		if !vtx.ParentsV[0].ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
	}

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if !bytes.Equal(b, vtx.Bytes()) {
			t.Fatalf("Wrong bytes")
		}
		return vtx, nil
	}
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(vtx.ParentsV[0].ID()) {
			return nil, errors.New("")
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errors.New("TODO")
	}

	te.Put(vdr, 0, vtx.ID(), vtx.Bytes())

	manager.ParseVertexF = nil

	if !*asked {
		t.Fatalf("Didn't ask for a missing vertex")
	}

	if len(te.vtxBlocked) != 1 {
		t.Fatalf("Should have been blocking on request")
	}

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) { return nil, errFailedParsing }

	te.Put(vdr, *reqID, vtx.ParentsV[0].ID(), nil)

	manager.ParseVertexF = nil

	if len(te.vtxBlocked) != 0 {
		t.Fatalf("Should have finished blocking issue")
	}
}

func TestEngineQuery(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}
	utxos := []ids.ID{ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	vtx0 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
		BytesV:   []byte{0, 1, 2, 3},
	}

	manager.EdgeF = func() []ids.ID { return []ids.ID{vts[0].ID(), vts[1].ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}

		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	vertexed := new(bool)
	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		*vertexed = true
		switch {
		case vtxID.Equals(gVtx.ID()):
			return gVtx, nil
		case vtxID.Equals(mVtx.ID()):
			return mVtx, nil
		}
		return nil, errUnknownVertex
	}

	asked := new(bool)
	sender.GetF = func(inVdr ids.ShortID, _ uint32, vtxID ids.ID) {
		if *asked {
			t.Fatalf("Asked multiple times")
		}
		*asked = true
		if !vdr.Equals(inVdr) {
			t.Fatalf("Asking wrong validator for vertex")
		}
		if !vtx0.ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
	}

	te.PullQuery(vdr, 0, vtx0.ID())
	if !*vertexed {
		t.Fatalf("Didn't request vertex")
	}
	if !*asked {
		t.Fatalf("Didn't request vertex from validator")
	}

	queried := new(bool)
	queryRequestID := new(uint32)
	sender.PushQueryF = func(inVdrs ids.ShortSet, requestID uint32, vtxID ids.ID, vtx []byte) {
		if *queried {
			t.Fatalf("Asked multiple times")
		}
		*queried = true
		*queryRequestID = requestID
		vdrSet := ids.ShortSet{}
		vdrSet.Add(vdr)
		if !inVdrs.Equals(vdrSet) {
			t.Fatalf("Asking wrong validator for preference")
		}
		if !vtx0.ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
	}

	chitted := new(bool)
	sender.ChitsF = func(inVdr ids.ShortID, _ uint32, prefs ids.Set) {
		if *chitted {
			t.Fatalf("Sent multiple chits")
		}
		*chitted = true
		if prefs.Len() != 1 || !prefs.Contains(vtx0.ID()) {
			t.Fatalf("Wrong chits preferences")
		}
	}

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if !bytes.Equal(b, vtx0.Bytes()) {
			t.Fatalf("Wrong bytes")
		}
		return vtx0, nil
	}
	te.Put(vdr, 0, vtx0.ID(), vtx0.Bytes())
	manager.ParseVertexF = nil

	if !*queried {
		t.Fatalf("Didn't ask for preferences")
	}
	if !*chitted {
		t.Fatalf("Didn't provide preferences")
	}

	vtx1 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
		BytesV:   []byte{5, 4, 3, 2, 1, 9},
	}

	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(gVtx.ID()):
			return gVtx, nil
		case vtxID.Equals(mVtx.ID()):
			return mVtx, nil
		case vtxID.Equals(vtx1.ID()):
			return nil, errUnknownVertex
		case vtxID.Equals(vtx0.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Wrong vertex requested")
		panic("Should have failed")
	}

	*asked = false
	sender.GetF = func(inVdr ids.ShortID, _ uint32, vtxID ids.ID) {
		if *asked {
			t.Fatalf("Asked multiple times")
		}
		*asked = true
		if !vdr.Equals(inVdr) {
			t.Fatalf("Asking wrong validator for vertex")
		}
		if !vtx1.ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
	}

	s := ids.Set{}
	s.Add(vtx1.ID())
	te.Chits(vdr, *queryRequestID, s)

	*queried = false
	sender.PushQueryF = func(inVdrs ids.ShortSet, requestID uint32, vtxID ids.ID, vtx []byte) {
		if *queried {
			t.Fatalf("Asked multiple times")
		}
		*queried = true
		*queryRequestID = requestID
		vdrSet := ids.ShortSet{}
		vdrSet.Add(vdr)
		if !inVdrs.Equals(vdrSet) {
			t.Fatalf("Asking wrong validator for preference")
		}
		if !vtx1.ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
	}

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if !bytes.Equal(b, vtx1.Bytes()) {
			t.Fatalf("Wrong bytes")
		}

		return vtx1, nil
	}
	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		if vtx.ID().Equals(vtx1.ID()) {
			return nil
		} else if vtx.ID().Equals(vtx0.ID()) {
			return nil
		}
		t.Fatal("saved wrong vertex")
		return errors.New("")
	}
	te.Put(vdr, 0, vtx1.ID(), vtx1.Bytes())
	manager.ParseVertexF = nil

	if vtx0.Status() != choices.Accepted {
		t.Fatalf("Should have executed vertex")
	} else if vtx1.Status() != choices.Accepted {
		t.Fatalf("Should have executed vertex")
	} else if len(te.vtxBlocked) != 0 {
		t.Fatalf("Should have finished blocking")
	}

	_ = te.polls.String() // Shouldn't panic

	te.QueryFailed(vdr, *queryRequestID)
	if len(te.vtxBlocked) != 0 {
		t.Fatalf("Should have finished blocking")
	}
}

func TestEngineMultipleQuery(t *testing.T) {
	config := DefaultConfig()

	config.Params = avalanche.Parameters{
		Parameters: snowball.Parameters{
			Metrics:           prometheus.NewRegistry(),
			K:                 3,
			Alpha:             2,
			BetaVirtuous:      1,
			BetaRogue:         2,
			ConcurrentRepolls: 1,
		},
		Parents:   2,
		BatchSize: 1,
	}

	vals := validators.NewSet()
	config.Validators = vals

	vdr0 := ids.GenerateTestShortID()
	vdr1 := ids.GenerateTestShortID()
	vdr2 := ids.GenerateTestShortID()

	vals.AddWeight(vdr0, 1)
	vals.AddWeight(vdr1, 1)
	vals.AddWeight(vdr2, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}
	utxos := []ids.ID{ids.GenerateTestID()}

	manager.EdgeF = func() []ids.ID { return []ids.ID{vts[0].ID(), vts[1].ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	vtx0 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	queried := new(bool)
	queryRequestID := new(uint32)
	sender.PushQueryF = func(inVdrs ids.ShortSet, requestID uint32, vtxID ids.ID, vtx []byte) {
		if *queried {
			t.Fatalf("Asked multiple times")
		}
		*queried = true
		*queryRequestID = requestID
		vdrSet := ids.ShortSet{}
		vdrSet.Add(vdr0, vdr1, vdr2)
		if !inVdrs.Equals(vdrSet) {
			t.Fatalf("Asking wrong validator for preference")
		}
		if !vtx0.ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
	}

	te.issue(vtx0)

	vtx1 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		case id.Equals(vtx0.ID()):
			return nil, errUnknownVertex
		case id.Equals(vtx1.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	asked := new(bool)
	reqID := new(uint32)
	sender.GetF = func(inVdr ids.ShortID, requestID uint32, vtxID ids.ID) {
		*reqID = requestID
		if *asked {
			t.Fatalf("Asked multiple times")
		}
		*asked = true
		if !vdr0.Equals(inVdr) {
			t.Fatalf("Asking wrong validator for vertex")
		}
		if !vtx1.ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
	}

	s0 := ids.Set{}
	s0.Add(vtx0.ID())
	s0.Add(vtx1.ID())

	s2 := ids.Set{}
	s2.Add(vtx0.ID())

	te.Chits(vdr0, *queryRequestID, s0)
	te.QueryFailed(vdr1, *queryRequestID)
	te.Chits(vdr2, *queryRequestID, s2)

	// Should be dropped because the query was marked as failed
	te.Chits(vdr1, *queryRequestID, s0)

	te.GetFailed(vdr0, *reqID)

	if vtx0.Status() != choices.Accepted {
		t.Fatalf("Should have executed vertex")
	}
	if len(te.vtxBlocked) != 0 {
		t.Fatalf("Should have finished blocking")
	}
}

func TestEngineBlockedIssue(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}
	utxos := []ids.ID{ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	vtx0 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
	}

	vtx1 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{
			&avalanche.TestVertex{TestDecidable: choices.TestDecidable{
				IDV:     vtx0.IDV,
				StatusV: choices.Unknown,
			}},
		},
		HeightV: 1,
		TxsV:    []snowstorm.Tx{tx0},
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if bytes.Equal(b, vtx1.Bytes()) {
			return vtx1, nil
		}
		t.Fatal("asked to parse wrong vertex")
		return nil, errors.New("TODO")
	}
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(vtx0.ID()) {
			return nil, errors.New("")
		} else if id.Equals(gVtx.ID()) {
			return gVtx, nil
		} else if id.Equals(mVtx.ID()) {
			return mVtx, nil
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errors.New("TODO")
	}
	te.PushQuery(vdr, 0, vtx1.ID(), vtx1.Bytes())

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if bytes.Equal(b, vtx0.Bytes()) {
			return vtx0, nil
		}
		t.Fatal("asked to parse wrong vertex")
		return nil, errors.New("TODO")
	}

	te.PushQuery(vdr, 1, vtx0.ID(), vtx0.Bytes())

	if prefs := te.Consensus.Preferences(); prefs.Len() != 1 || !prefs.Contains(vtx1.ID()) {
		t.Fatalf("Should have issued vtx1")
	}
}

func TestEngineAbandonResponse(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}
	utxos := []ids.ID{ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) { return nil, errUnknownVertex }

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	reqID := new(uint32)
	sender.GetF = func(vID ids.ShortID, requestID uint32, vtxID ids.ID) {
		*reqID = requestID
	}

	te.PullQuery(vdr, 0, vtx.ID())
	te.GetFailed(vdr, *reqID)

	if len(te.vtxBlocked) != 0 {
		t.Fatalf("Should have removed blocking event")
	}
}

func TestEngineScheduleRepoll(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}
	utxos := []ids.ID{ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
	}

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)
	manager.CantEdge = false

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	requestID := new(uint32)
	sender.PushQueryF = func(_ ids.ShortSet, reqID uint32, _ ids.ID, _ []byte) {
		*requestID = reqID
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(gVtx.ID()) {
			return gVtx, nil
		} else if id.Equals(mVtx.ID()) {
			return mVtx, nil
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errors.New("")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if bytes.Equal(b, vtx.Bytes()) {
			return vtx, nil
		}
		t.Fatal("asked to parse wrong vertex")
		return nil, errors.New("")
	}
	sender.ChitsF = func(ids.ShortID, uint32, ids.Set) {}
	te.PushQuery(vdr, 0, vtx.ID(), vtx.Bytes())

	sender.PushQueryF = nil

	repolled := new(bool)
	sender.PullQueryF = func(_ ids.ShortSet, _ uint32, vtxID ids.ID) {
		*repolled = true
		if !vtxID.Equals(vtx.ID()) {
			t.Fatalf("Wrong vertex queried")
		}
	}

	te.QueryFailed(vdr, *requestID)

	if !*repolled {
		t.Fatalf("Should have issued a noop")
	}
}

func TestEngineRejectDoubleSpendTx(t *testing.T) {
	config := DefaultConfig()

	config.Params.BatchSize = 2

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	vm := &vertex.TestVM{}
	vm.T = t
	config.VM = vm

	vm.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	gTx := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	utxos := []ids.ID{ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx1.InputIDsV.Add(utxos[0])

	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID(), mVtx.ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.BuildVertexF = func(_ ids.Set, txs []snowstorm.Tx) (avalanche.Vertex, error) {
		return &avalanche.TestVertex{
			TestDecidable: choices.TestDecidable{
				IDV:     ids.GenerateTestID(),
				StatusV: choices.Processing,
			},
			ParentsV: []avalanche.Vertex{gVtx, mVtx},
			HeightV:  1,
			TxsV:     txs,
			BytesV:   []byte{1},
		}, nil
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	sender.CantPushQuery = false

	vm.PendingTxsF = func() []snowstorm.Tx { return []snowstorm.Tx{tx0, tx1} }
	te.Notify(common.PendingTxs)
}

func TestEngineRejectDoubleSpendIssuedTx(t *testing.T) {
	config := DefaultConfig()

	config.Params.BatchSize = 2

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	vm := &vertex.TestVM{}
	vm.T = t
	config.VM = vm

	vm.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	gTx := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	utxos := []ids.ID{ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx1.InputIDsV.Add(utxos[0])

	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID(), mVtx.ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	manager.BuildVertexF = func(_ ids.Set, txs []snowstorm.Tx) (avalanche.Vertex, error) {
		return &avalanche.TestVertex{
			TestDecidable: choices.TestDecidable{
				IDV:     ids.GenerateTestID(),
				StatusV: choices.Processing,
			},
			ParentsV: []avalanche.Vertex{gVtx, mVtx},
			HeightV:  1,
			TxsV:     txs,
			BytesV:   []byte{1},
		}, nil
	}

	sender.CantPushQuery = false

	vm.PendingTxsF = func() []snowstorm.Tx { return []snowstorm.Tx{tx0} }
	te.Notify(common.PendingTxs)

	vm.PendingTxsF = func() []snowstorm.Tx { return []snowstorm.Tx{tx1} }
	te.Notify(common.PendingTxs)
}

func TestEngineIssueRepoll(t *testing.T) {
	config := DefaultConfig()

	config.Params.BatchSize = 2

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID(), mVtx.ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	sender.PullQueryF = func(vdrs ids.ShortSet, _ uint32, vtxID ids.ID) {
		vdrSet := ids.ShortSet{}
		vdrSet.Add(vdr)
		if !vdrs.Equals(vdrSet) {
			t.Fatalf("Wrong query recipients")
		}
		if !vtxID.Equals(gVtx.ID()) && !vtxID.Equals(mVtx.ID()) {
			t.Fatalf("Unknown re-query")
		}
	}

	te.repoll()
}

func TestEngineReissue(t *testing.T) {
	config := DefaultConfig()
	config.Params.BatchSize = 2
	config.Params.BetaVirtuous = 5
	config.Params.BetaRogue = 5

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	vm := &vertex.TestVM{}
	vm.T = t
	config.VM = vm

	vm.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	gTx := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx1.InputIDsV.Add(utxos[1])

	tx2 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx2.InputIDsV.Add(utxos[1])

	tx3 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx3.InputIDsV.Add(utxos[0])

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{gVtx, mVtx},
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx2},
		BytesV:   []byte{42},
	}

	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID(), mVtx.ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		case id.Equals(vtx.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	lastVtx := new(avalanche.TestVertex)
	manager.BuildVertexF = func(_ ids.Set, txs []snowstorm.Tx) (avalanche.Vertex, error) {
		lastVtx = &avalanche.TestVertex{
			TestDecidable: choices.TestDecidable{
				IDV:     ids.GenerateTestID(),
				StatusV: choices.Processing,
			},
			ParentsV: []avalanche.Vertex{gVtx, mVtx},
			HeightV:  1,
			TxsV:     txs,
			BytesV:   []byte{1},
		}
		return lastVtx, nil
	}

	vm.GetTxF = func(id ids.ID) (snowstorm.Tx, error) {
		if !id.Equals(tx0.ID()) {
			t.Fatalf("Wrong tx")
		}
		return tx0, nil
	}

	queryRequestID := new(uint32)
	sender.PushQueryF = func(_ ids.ShortSet, requestID uint32, _ ids.ID, _ []byte) {
		*queryRequestID = requestID
	}

	vm.PendingTxsF = func() []snowstorm.Tx { return []snowstorm.Tx{tx0, tx1} }
	te.Notify(common.PendingTxs)

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if !bytes.Equal(b, vtx.Bytes()) {
			t.Fatalf("Wrong bytes")
		}
		return vtx, nil
	}
	te.Put(vdr, 0, vtx.ID(), vtx.Bytes())
	manager.ParseVertexF = nil

	vm.PendingTxsF = func() []snowstorm.Tx { return []snowstorm.Tx{tx3} }
	te.Notify(common.PendingTxs)

	s := ids.Set{}
	s.Add(vtx.ID())
	te.Chits(vdr, *queryRequestID, s)

	if len(lastVtx.TxsV) != 1 || !lastVtx.TxsV[0].ID().Equals(tx0.ID()) {
		t.Fatalf("Should have re-issued the tx")
	}
}

func TestEngineLargeIssue(t *testing.T) {
	config := DefaultConfig()
	config.Params.BatchSize = 1
	config.Params.BetaVirtuous = 5
	config.Params.BetaRogue = 5

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	vm := &vertex.TestVM{}
	vm.T = t
	config.VM = vm

	vm.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	gTx := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx1.InputIDsV.Add(utxos[1])

	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID(), mVtx.ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	lastVtx := new(avalanche.TestVertex)
	manager.BuildVertexF = func(_ ids.Set, txs []snowstorm.Tx) (avalanche.Vertex, error) {
		lastVtx = &avalanche.TestVertex{
			TestDecidable: choices.TestDecidable{
				IDV:     ids.GenerateTestID(),
				StatusV: choices.Processing,
			},
			ParentsV: []avalanche.Vertex{gVtx, mVtx},
			HeightV:  1,
			TxsV:     txs,
			BytesV:   []byte{1},
		}
		return lastVtx, nil
	}

	sender.CantPushQuery = false

	vm.PendingTxsF = func() []snowstorm.Tx { return []snowstorm.Tx{tx0, tx1} }
	te.Notify(common.PendingTxs)

	if len(lastVtx.TxsV) != 1 || !lastVtx.TxsV[0].ID().Equals(tx1.ID()) {
		t.Fatalf("Should have issued txs differently")
	}
}

func TestEngineGetVertex(t *testing.T) {
	config := DefaultConfig()

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	vdr := validators.GenerateRandomValidator(1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID(), mVtx.ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	sender.PutF = func(v ids.ShortID, _ uint32, vtxID ids.ID, vtx []byte) {
		if !v.Equals(vdr.ID()) {
			t.Fatalf("Wrong validator")
		}
		if !mVtx.ID().Equals(vtxID) {
			t.Fatalf("Wrong vertex")
		}
	}

	te.Get(vdr.ID(), 0, mVtx.ID())
}

func TestEnginePushGossip(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		BytesV:   []byte{0, 1, 2, 3},
	}

	manager.EdgeF = func() []ids.ID { return []ids.ID{vts[0].ID(), vts[1].ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		case id.Equals(vtx.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	requested := new(bool)
	sender.GetF = func(vdr ids.ShortID, _ uint32, vtxID ids.ID) {
		*requested = true
	}

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if bytes.Equal(b, vtx.Bytes()) {
			return vtx, nil
		}
		t.Fatalf("Unknown vertex bytes")
		panic("Should have errored")
	}
	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		if vtx.ID().Equals(vtx.ID()) {
			return nil
		}
		t.Fatalf("Unknown vertex")
		return errUnknownVertex
	}

	sender.CantPushQuery = false
	sender.CantChits = false
	te.PushQuery(vdr, 0, vtx.ID(), vtx.Bytes())

	if *requested {
		t.Fatalf("Shouldn't have requested the vertex")
	}
}

func TestEngineParentBlockingInsert(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}

	missingVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Unknown,
		},
		ParentsV: vts,
		HeightV:  1,
		BytesV:   []byte{0, 1, 2, 3},
	}

	parentVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{missingVtx},
		HeightV:  2,
		BytesV:   []byte{0, 1, 2, 3, 4},
	}

	blockingVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{parentVtx},
		HeightV:  3,
		BytesV:   []byte{0, 1, 2, 3, 4, 5},
	}

	manager.EdgeF = func() []ids.ID { return []ids.ID{vts[0].ID(), vts[1].ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(gVtx.ID()):
			return gVtx, nil
		case vtxID.Equals(mVtx.ID()):
			return mVtx, nil
		case vtxID.Equals(parentVtx.ID()):
			return nil, errUnknownVertex
		case vtxID.Equals(missingVtx.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, parentVtx.Bytes()):
			return parentVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	sender.GetF = func(_ ids.ShortID, _ uint32, id ids.ID) {
		if id.Equals(missingVtx.ID()) {
			return
		}
		t.Fatal("should have tried to get missingVtx")
	}
	te.PushQuery(vdr, 0, parentVtx.ID(), parentVtx.Bytes())

	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(gVtx.ID()):
			return gVtx, nil
		case vtxID.Equals(mVtx.ID()):
			return mVtx, nil
		case vtxID.Equals(parentVtx.ID()):
			return nil, errUnknownVertex
		case vtxID.Equals(blockingVtx.ID()):
			return nil, errUnknownVertex
		case vtxID.Equals(missingVtx.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, blockingVtx.Bytes()):
			return blockingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	te.PushQuery(vdr, 1, blockingVtx.ID(), blockingVtx.Bytes())

	if len(te.vtxBlocked) != 3 {
		t.Fatalf("# blocked is %d but should be %d", len(te.vtxBlocked), 3)
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, missingVtx.Bytes()):
			return missingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		switch {
		case vtx.ID().Equals(missingVtx.ID()):
			return nil
		case vtx.ID().Equals(blockingVtx.ID()):
			return nil
		case vtx.ID().Equals(parentVtx.ID()):
			return nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(gVtx.ID()):
			return gVtx, nil
		case vtxID.Equals(mVtx.ID()):
			return mVtx, nil
		case vtxID.Equals(parentVtx.ID()):
			return parentVtx, nil
		case vtxID.Equals(blockingVtx.ID()):
			return blockingVtx, nil
		case vtxID.Equals(missingVtx.ID()):
			return missingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	sender.CantPushQuery = false
	sender.ChitsF = func(ids.ShortID, uint32, ids.Set) {}

	missingVtx.StatusV = choices.Processing
	if err := te.PushQuery(vdr, 2, missingVtx.ID(), missingVtx.Bytes()); err != nil {
		t.Fatal(err)
	}

	if len(te.vtxBlocked) != 0 {
		t.Fatalf("Both inserts should not longer be blocking")
	}
}

func TestEngineBlockingChitRequest(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false
	sender.ChitsF = func(ids.ShortID, uint32, ids.Set) {}

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}

	missingVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Unknown,
		},
		ParentsV: vts,
		HeightV:  1,
		BytesV:   []byte{0, 1, 2, 3},
	}

	parentVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{missingVtx},
		HeightV:  2,
		BytesV:   []byte{1, 1, 2, 3},
	}

	blockingVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{parentVtx},
		HeightV:  3,
		BytesV:   []byte{2, 1, 2, 3},
	}

	manager.EdgeF = func() []ids.ID { return []ids.ID{vts[0].ID(), vts[1].ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		case id.Equals(missingVtx.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, parentVtx.Bytes()):
			return parentVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	sender.GetF = func(_ ids.ShortID, reqID uint32, vtxID ids.ID) {
		if !vtxID.Equals(missingVtx.ID()) {
			t.Fatal("should have asked for missingVtx")
		}
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	if err := te.PushQuery(vdr, 0, parentVtx.ID(), parentVtx.Bytes()); err != nil {
		t.Fatal(err)
	}

	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(blockingVtx.ID()):
			return blockingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, blockingVtx.Bytes()):
			return blockingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te.PushQuery(vdr, 0, blockingVtx.ID(), blockingVtx.Bytes())

	if len(te.vtxBlocked) != 3 {
		t.Fatalf("Both inserts and the query should be blocking")
	}

	sender.CantPushQuery = false
	sender.CantChits = false

	missingVtx.StatusV = choices.Processing

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		case id.Equals(blockingVtx.ID()):
			return blockingVtx, nil
		case id.Equals(missingVtx.ID()):
			return missingVtx, nil
		case id.Equals(parentVtx.ID()):
			return parentVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, missingVtx.Bytes()):
			return missingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		if vtx.ID().Equals(missingVtx.ID()) {
			return nil
		} else if vtx.ID().Equals(parentVtx.ID()) {
			return nil
		} else if vtx.ID().Equals(blockingVtx.ID()) {
			return nil
		}
		t.Fatal("saved wrong vertex")
		return errors.New("")
	}
	if err := te.PushQuery(vdr, 1, missingVtx.ID(), missingVtx.Bytes()); err != nil {
		t.Fatal(err)
	}

	if len(te.vtxBlocked) != 0 {
		t.Fatalf("nothing should be blocking")
	}
}

func TestEngineBlockingChitResponse(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}

	issuedVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		BytesV:   []byte{0, 1, 2, 3},
	}

	missingVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Unknown,
		},
		ParentsV: vts,
		HeightV:  1,
		BytesV:   []byte{1, 1, 2, 3},
	}

	blockingVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{missingVtx},
		HeightV:  2,
		BytesV:   []byte{2, 1, 2, 3},
	}

	manager.EdgeF = func() []ids.ID { return []ids.ID{vts[0].ID(), vts[1].ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		case id.Equals(missingVtx.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, blockingVtx.Bytes()):
			return blockingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	sender.ChitsF = func(ids.ShortID, uint32, ids.Set) {}
	sender.GetF = func(_ ids.ShortID, _ uint32, id ids.ID) {
		if !id.Equals(missingVtx.ID()) {
			t.Fatal("asked for wrong vertex")
		}
	}

	if err := te.PushQuery(vdr, 0, blockingVtx.ID(), blockingVtx.Bytes()); err != nil {
		t.Fatal(err)
	}

	queryRequestID := new(uint32)
	sender.PushQueryF = func(inVdrs ids.ShortSet, requestID uint32, vtxID ids.ID, vtx []byte) {
		*queryRequestID = requestID
		vdrSet := ids.ShortSet{}
		vdrSet.Add(vdr)
		if !inVdrs.Equals(vdrSet) {
			t.Fatalf("Asking wrong validator for preference")
		}
		if !issuedVtx.ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, issuedVtx.Bytes()):
			return issuedVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		if vtx.ID().Equals(issuedVtx.ID()) {
			return nil
		}
		t.Fatal("saved wrong vertex")
		return errUnknownVertex
	}

	if err := te.PushQuery(vdr, 1, issuedVtx.ID(), issuedVtx.Bytes()); err != nil {
		t.Fatal(err)
	}

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, missingVtx.Bytes()):
			return missingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		case id.Equals(missingVtx.ID()):
			return missingVtx, nil
		case id.Equals(issuedVtx.ID()):
			return issuedVtx, nil
		case id.Equals(blockingVtx.ID()):
			return blockingVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		if vtx.ID().Equals(missingVtx.ID()) {
			return nil
		} else if vtx.ID().Equals(blockingVtx.ID()) {
			return nil
		}
		t.Fatal("saved wrong vertex")
		return errUnknownVertex
	}

	voteSet := ids.Set{}
	voteSet.Add(blockingVtx.ID())
	te.Chits(vdr, *queryRequestID, voteSet)

	if len(te.vtxBlocked) != 2 {
		t.Fatalf("The insert should be blocking, as well as the chit response")
	}

	sender.PushQueryF = nil
	sender.CantPushQuery = false
	sender.CantChits = false

	missingVtx.StatusV = choices.Processing

	if err := te.PushQuery(vdr, 2, missingVtx.ID(), missingVtx.Bytes()); err != nil {
		t.Fatal(err)
	}

	if len(te.vtxBlocked) != 0 {
		t.Fatalf("Both inserts should not longer be blocking")
	}
}

func TestEngineIssueBlockingTx(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx}
	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{tx0},
	}
	tx1.InputIDsV.Add(utxos[1])

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0, tx1},
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx.Bytes()):
			return vtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	if err := te.PushQuery(vdr, 0, vtx.ID(), vtx.Bytes()); err != nil {
		t.Fatal(err)
	}

	if prefs := te.Consensus.Preferences(); !prefs.Contains(vtx.ID()) {
		t.Fatalf("Vertex should be preferred")
	}
}

func TestEngineReissueAbortedVertex(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx}

	vtxID0 := ids.GenerateTestID()
	vtxID1 := ids.GenerateTestID()

	vtxBytes0 := []byte{0}
	vtxBytes1 := []byte{1}

	vtx0 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     vtxID0,
			StatusV: choices.Unknown,
		},
		ParentsV: vts,
		HeightV:  1,
		BytesV:   vtxBytes0,
	}
	vtx1 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     vtxID1,
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx0},
		HeightV:  2,
		BytesV:   vtxBytes1,
	}

	manager.EdgeF = func() []ids.ID {
		return []ids.ID{gVtx.ID()}
	}

	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(gVtx.ID()):
			return gVtx, nil
		}
		t.Fatalf("Unknown vertex requested")
		panic("Unknown vertex requested")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	manager.EdgeF = nil
	manager.GetVertexF = nil

	requestID := new(uint32)
	sender.GetF = func(vID ids.ShortID, reqID uint32, vtxID ids.ID) {
		*requestID = reqID
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtxBytes1):
			return vtx1, nil
		}
		t.Fatalf("Unknown bytes provided")
		panic("Unknown bytes provided")
	}
	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(vtxID1):
			return nil, errUnknownVertex
		case vtxID.Equals(vtxID0):
			return nil, errUnknownVertex
		case vtxID.Equals(gVtx.ID()):
			return gVtx, nil

		}
		t.Fatalf("Unknown bytes provided")
		panic("Unknown bytes provided")
	}

	te.PushQuery(vdr, 0, vtxID1, vtx1.Bytes())

	sender.GetF = nil
	manager.ParseVertexF = nil

	te.GetFailed(vdr, *requestID)

	requested := new(bool)
	sender.GetF = func(_ ids.ShortID, _ uint32, vtxID ids.ID) {
		if vtxID.Equals(vtxID0) {
			*requested = true
		}
	}

	te.PullQuery(vdr, 0, vtxID1)

	if !*requested {
		t.Fatalf("Should have requested the missing vertex")
	}
}

func TestEngineBootstrappingIntoConsensus(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals
	config.Beacons = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	vm := &vertex.TestVM{}
	vm.T = t
	config.VM = vm

	vm.Default(true)

	vm.CantBootstrapping = false
	vm.CantBootstrapped = false

	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	txID0 := ids.GenerateTestID()
	txID1 := ids.GenerateTestID()

	txBytes0 := []byte{0}
	txBytes1 := []byte{1}

	tx0 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     txID0,
			StatusV: choices.Processing,
		},
		BytesV: txBytes0,
	}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     txID1,
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{tx0},
		BytesV:        txBytes1,
	}
	tx1.InputIDsV.Add(utxos[1])

	vtxID0 := ids.GenerateTestID()
	vtxID1 := ids.GenerateTestID()

	vtxBytes0 := []byte{2}
	vtxBytes1 := []byte{3}

	vtx0 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     vtxID0,
			StatusV: choices.Processing,
		},
		HeightV: 1,
		TxsV:    []snowstorm.Tx{tx0},
		BytesV:  vtxBytes0,
	}
	vtx1 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     vtxID1,
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx0},
		HeightV:  2,
		TxsV:     []snowstorm.Tx{tx1},
		BytesV:   vtxBytes1,
	}

	requested := new(bool)
	requestID := new(uint32)
	sender.GetAcceptedFrontierF = func(vdrs ids.ShortSet, reqID uint32) {
		if vdrs.Len() != 1 {
			t.Fatalf("Should have requested from the validators")
		}
		if !vdrs.Contains(vdr) {
			t.Fatalf("Should have requested from %s", vdr)
		}
		*requested = true
		*requestID = reqID
	}

	te := &Transitive{}
	te.Initialize(config)
	te.Startup()

	sender.GetAcceptedFrontierF = nil

	if !*requested {
		t.Fatalf("Should have requested from the validators during Initialize")
	}

	acceptedFrontier := ids.Set{}
	acceptedFrontier.Add(vtxID0)

	*requested = false
	sender.GetAcceptedF = func(vdrs ids.ShortSet, reqID uint32, proposedAccepted ids.Set) {
		if vdrs.Len() != 1 {
			t.Fatalf("Should have requested from the validators")
		}
		if !vdrs.Contains(vdr) {
			t.Fatalf("Should have requested from %s", vdr)
		}
		if !acceptedFrontier.Equals(proposedAccepted) {
			t.Fatalf("Wrong proposedAccepted vertices.\nExpected: %s\nGot: %s", acceptedFrontier, proposedAccepted)
		}
		*requested = true
		*requestID = reqID
	}

	te.AcceptedFrontier(vdr, *requestID, acceptedFrontier)

	if !*requested {
		t.Fatalf("Should have requested from the validators during AcceptedFrontier")
	}

	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(vtxID0):
			return nil, errMissing
		}
		t.Fatalf("Unknown vertex requested")
		panic("Unknown vertex requested")
	}

	sender.GetAncestorsF = func(inVdr ids.ShortID, reqID uint32, vtxID ids.ID) {
		if !vdr.Equals(inVdr) {
			t.Fatalf("Asking wrong validator for vertex")
		}
		if !vtx0.ID().Equals(vtxID) {
			t.Fatalf("Asking for wrong vertex")
		}
		*requestID = reqID
	}

	te.Accepted(vdr, *requestID, acceptedFrontier)

	manager.GetVertexF = nil
	sender.GetF = nil

	vm.ParseTxF = func(b []byte) (snowstorm.Tx, error) {
		switch {
		case bytes.Equal(b, txBytes0):
			return tx0, nil
		}
		t.Fatalf("Unknown bytes provided")
		panic("Unknown bytes provided")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtxBytes0):
			return vtx0, nil
		}
		t.Fatalf("Unknown bytes provided")
		panic("Unknown bytes provided")
	}
	manager.EdgeF = func() []ids.ID {
		return []ids.ID{vtxID0}
	}
	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(vtxID0):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown bytes provided")
		panic("Unknown bytes provided")
	}
	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		if vtx.ID().Equals(vtx0.ID()) {
			return nil
		}
		t.Fatal("saved wrong vertex")
		return errUnknownVertex
	}

	te.MultiPut(vdr, *requestID, [][]byte{vtxBytes0})

	vm.ParseTxF = nil
	manager.ParseVertexF = nil
	manager.EdgeF = nil
	manager.GetVertexF = nil

	if tx0.Status() != choices.Accepted {
		t.Fatalf("Should have accepted %s", txID0)
	}
	if vtx0.Status() != choices.Accepted {
		t.Fatalf("Should have accepted %s", vtxID0)
	}

	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtxBytes1):
			return vtx1, nil
		}
		t.Fatalf("Unknown bytes provided")
		panic("Unknown bytes provided")
	}
	sender.ChitsF = func(inVdr ids.ShortID, _ uint32, chits ids.Set) {
		if !inVdr.Equals(vdr) {
			t.Fatalf("Sent to the wrong validator")
		}

		expected := ids.Set{}
		expected.Add(vtxID1)

		if !expected.Equals(chits) {
			t.Fatalf("Returned wrong chits")
		}
	}
	sender.PushQueryF = func(vdrs ids.ShortSet, _ uint32, vtxID ids.ID, vtx []byte) {
		if vdrs.Len() != 1 {
			t.Fatalf("Should have requested from the validators")
		}
		if !vdrs.Contains(vdr) {
			t.Fatalf("Should have requested from %s", vdr)
		}

		if !vtxID1.Equals(vtxID) {
			t.Fatalf("Sent wrong query ID")
		}
		if !bytes.Equal(vtxBytes1, vtx) {
			t.Fatalf("Sent wrong query bytes")
		}
	}
	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(vtxID1):
			return vtx1, nil
		case vtxID.Equals(vtxID0):
			return vtx0, nil
		}
		t.Fatalf("Unknown bytes provided")
		panic("Unknown bytes provided")
	}

	if err := te.PushQuery(vdr, 0, vtxID1, vtxBytes1); err != nil {
		t.Fatal(err)
	}
}

/* TODO put this test back
func TestEngineUndeclaredDependencyDeadlock(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx}
	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		VerifyV: errors.New(""),
	}
	tx1.InputIDsV.Add(utxos[1])

	vtx0 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
	}
	vtx1 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx0},
		HeightV:  2,
		TxsV:     []snowstorm.Tx{tx1},
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	sender := &common.SenderTest{}
	sender.T = t
	te.Sender = sender

	reqID := new(uint32)
	sender.PushQueryF = func(_ ids.ShortSet, requestID uint32, _ ids.ID, _ []byte) {
		*reqID = requestID
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx0.Bytes()):
			return vtx0, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	sender.ChitsF = func(ids.ShortID, uint32, ids.Set) {}

	if err := te.PushQuery(vdr, 0, vtx0.ID(), vtx0.Bytes()); err != nil {
		t.Fatal(err)
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(vtx0.ID()):
			return nil, errUnknownVertex
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx1.Bytes()):
			return vtx1, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	if err := te.PushQuery(vdr, 1, vtx1.ID(), vtx1.Bytes()); err != nil {
		t.Fatal(err)
	}

	votes := ids.Set{}
	votes.Add(vtx1.ID())
	if err := te.Chits(vdr, *reqID, votes); err != nil {
		t.Fatal(err)
	}

	if status := vtx0.Status(); status != choices.Accepted {
		t.Fatalf("vtx0 should be %s but is %s", choices.Accepted, status)
	}
}
*/
func TestEnginePartiallyValidVertex(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx}
	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		VerifyV: errors.New(""),
	}
	tx1.InputIDsV.Add(utxos[1])

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0, tx1},
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	expectedVtxID := ids.GenerateTestID()
	manager.BuildVertexF = func(_ ids.Set, txs []snowstorm.Tx) (avalanche.Vertex, error) {
		return &avalanche.TestVertex{
			TestDecidable: choices.TestDecidable{
				IDV:     expectedVtxID,
				StatusV: choices.Processing,
			},
			ParentsV: vts,
			HeightV:  1,
			TxsV:     txs,
			BytesV:   []byte{1},
		}, nil
	}

	sender := &common.SenderTest{}
	sender.T = t
	te.Sender = sender

	sender.PushQueryF = func(_ ids.ShortSet, _ uint32, vtxID ids.ID, _ []byte) {
		if !expectedVtxID.Equals(vtxID) {
			t.Fatalf("wrong vertex queried")
		}
	}

	te.issue(vtx)
}

func TestEngineGossip(t *testing.T) {
	config := DefaultConfig()

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID()} }
	manager.GetVertexF = func(vtxID ids.ID) (avalanche.Vertex, error) {
		switch {
		case vtxID.Equals(gVtx.ID()):
			return gVtx, nil
		}
		t.Fatal(errUnknownVertex)
		return nil, errUnknownVertex
	}

	called := new(bool)
	sender.GossipF = func(vtxID ids.ID, vtxBytes []byte) {
		*called = true
		switch {
		case !vtxID.Equals(gVtx.ID()):
			t.Fatal(errUnknownVertex)
		}
		switch {
		case !bytes.Equal(vtxBytes, gVtx.Bytes()):
			t.Fatal(errUnknownVertex)
		}
	}

	te.Gossip()

	if !*called {
		t.Fatalf("Should have gossiped the vertex")
	}
}

func TestEngineInvalidVertexIgnoredFromUnexpectedPeer(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	secondVdr := ids.GenerateTestShortID()

	vals.AddWeight(vdr, 1)
	vals.AddWeight(secondVdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Accepted,
		},
		BytesV: []byte{0},
	}

	vts := []avalanche.Vertex{gVtx}
	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx1.InputIDsV.Add(utxos[1])

	vtx0 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Unknown,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
		BytesV:   []byte{1},
	}
	vtx1 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx0},
		HeightV:  2,
		TxsV:     []snowstorm.Tx{tx1},
		BytesV:   []byte{2},
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	parsed := new(bool)
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx1.Bytes()):
			*parsed = true
			return vtx1, nil
		}
		return nil, errUnknownVertex
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(gVtx.ID()) {
			return gVtx, nil
		} else if id.Equals(vtx0.ID()) {
			return nil, errUnknownVertex
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errors.New("")
	}

	reqID := new(uint32)
	sender.GetF = func(reqVdr ids.ShortID, requestID uint32, vtxID ids.ID) {
		*reqID = requestID
		if !reqVdr.Equals(vdr) {
			t.Fatalf("Wrong validator requested")
		}
		if !vtxID.Equals(vtx0.ID()) {
			t.Fatalf("Wrong vertex requested")
		}
	}

	te.PushQuery(vdr, 0, vtx1.ID(), vtx1.Bytes())

	te.Put(secondVdr, *reqID, vtx0.ID(), []byte{3})

	*parsed = false
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx0.Bytes()):
			*parsed = true
			return vtx0, nil
		}
		return nil, errUnknownVertex
	}

	sender.CantPushQuery = false
	sender.CantChits = false

	vtx0.StatusV = choices.Processing

	te.Put(vdr, *reqID, vtx0.ID(), vtx0.Bytes())

	prefs := te.Consensus.Preferences()
	if !prefs.Contains(vtx1.ID()) {
		t.Fatalf("Shouldn't have abandoned the pending vertex")
	}
}

func TestEnginePushQueryRequestIDConflict(t *testing.T) {
	config := DefaultConfig()

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Accepted,
		},
		BytesV: []byte{0},
	}

	vts := []avalanche.Vertex{gVtx}
	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx1.InputIDsV.Add(utxos[1])

	vtx0 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Unknown,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
		BytesV:   []byte{1},
	}

	vtx1 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx0},
		HeightV:  2,
		TxsV:     []snowstorm.Tx{tx1},
		BytesV:   []byte{2},
	}

	randomVtxID := ids.GenerateTestID()

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	parsed := new(bool)
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx1.Bytes()):
			*parsed = true
			return vtx1, nil
		}
		return nil, errUnknownVertex
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(gVtx.ID()) {
			return gVtx, nil
		} else if id.Equals(vtx0.ID()) {
			return nil, errUnknownVertex
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errors.New("")
	}

	reqID := new(uint32)
	sender.GetF = func(reqVdr ids.ShortID, requestID uint32, vtxID ids.ID) {
		*reqID = requestID
		if !reqVdr.Equals(vdr) {
			t.Fatalf("Wrong validator requested")
		}
		if !vtxID.Equals(vtx0.ID()) {
			t.Fatalf("Wrong vertex requested")
		}
	}

	te.PushQuery(vdr, 0, vtx1.ID(), vtx1.Bytes())

	sender.GetF = nil
	sender.CantGet = false

	te.PushQuery(vdr, *reqID, randomVtxID, []byte{3})

	*parsed = false
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx0.Bytes()):
			*parsed = true
			return vtx0, nil
		}
		return nil, errUnknownVertex
	}

	sender.CantPushQuery = false
	sender.CantChits = false

	vtx0.StatusV = choices.Processing

	te.Put(vdr, *reqID, vtx0.ID(), vtx0.Bytes())

	prefs := te.Consensus.Preferences()
	if !prefs.Contains(vtx1.ID()) {
		t.Fatalf("Shouldn't have abandoned the pending vertex")
	}
}

func TestEngineAggressivePolling(t *testing.T) {
	config := DefaultConfig()

	config.Params.ConcurrentRepolls = 3
	config.Params.BetaRogue = 3

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	gVtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Accepted,
		},
		BytesV: []byte{0},
	}

	vts := []avalanche.Vertex{gVtx}
	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx0 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx0.InputIDsV.Add(utxos[0])

	tx1 := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx1.InputIDsV.Add(utxos[1])

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx0},
		BytesV:   []byte{1},
	}

	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(gVtx.ID()) {
			return gVtx, nil
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errors.New("")
	}

	te := &Transitive{}
	if err := te.Initialize(config); err != nil {
		t.Fatal(err)
	}
	if err := te.finishBootstrapping(); err != nil {
		t.Fatal(err)
	}
	te.Ctx.Bootstrapped()

	parsed := new(bool)
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx.Bytes()):
			*parsed = true
			return vtx, nil
		}
		return nil, errUnknownVertex
	}

	numPushQueries := new(int)
	sender.PushQueryF = func(ids.ShortSet, uint32, ids.ID, []byte) { *numPushQueries++ }

	numPullQueries := new(int)
	sender.PullQueryF = func(ids.ShortSet, uint32, ids.ID) { *numPullQueries++ }

	te.Put(vdr, 0, vtx.ID(), vtx.Bytes())

	if *numPushQueries != 1 {
		t.Fatalf("should have issued one push query")
	}
	if *numPullQueries != 2 {
		t.Fatalf("should have issued two pull queries")
	}
}

func TestEngineDuplicatedIssuance(t *testing.T) {
	config := DefaultConfig()
	config.Params.BatchSize = 1
	config.Params.BetaVirtuous = 5
	config.Params.BetaRogue = 5

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	vals := validators.NewSet()
	config.Validators = vals

	vdr := ids.GenerateTestShortID()
	vals.AddWeight(vdr, 1)

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	vm := &vertex.TestVM{}
	vm.T = t
	config.VM = vm

	vm.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	gTx := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	utxos := []ids.ID{ids.GenerateTestID(), ids.GenerateTestID()}

	tx := &snowstorm.TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		DependenciesV: []snowstorm.Tx{gTx},
	}
	tx.InputIDsV.Add(utxos[0])

	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID(), mVtx.ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	lastVtx := new(avalanche.TestVertex)
	manager.BuildVertexF = func(_ ids.Set, txs []snowstorm.Tx) (avalanche.Vertex, error) {
		lastVtx = &avalanche.TestVertex{
			TestDecidable: choices.TestDecidable{
				IDV:     ids.GenerateTestID(),
				StatusV: choices.Processing,
			},
			ParentsV: []avalanche.Vertex{gVtx, mVtx},
			HeightV:  1,
			TxsV:     txs,
			BytesV:   []byte{1},
		}
		return lastVtx, nil
	}

	sender.CantPushQuery = false

	vm.PendingTxsF = func() []snowstorm.Tx { return []snowstorm.Tx{tx} }
	te.Notify(common.PendingTxs)

	if len(lastVtx.TxsV) != 1 || !lastVtx.TxsV[0].ID().Equals(tx.ID()) {
		t.Fatalf("Should have issued txs differently")
	}

	manager.BuildVertexF = func(ids.Set, []snowstorm.Tx) (avalanche.Vertex, error) {
		t.Fatalf("shouldn't have attempted to issue a duplicated tx")
		return nil, nil
	}

	te.Notify(common.PendingTxs)
}

func TestEngineDoubleChit(t *testing.T) {
	config := DefaultConfig()

	config.Params.Alpha = 2
	config.Params.K = 2

	vals := validators.NewSet()
	config.Validators = vals

	vdr0 := ids.GenerateTestShortID()
	vdr1 := ids.GenerateTestShortID()

	vals.AddWeight(vdr0, 1)
	vals.AddWeight(vdr1, 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender

	sender.Default(true)
	sender.CantGetAcceptedFrontier = false

	manager := &vertex.TestManager{T: t}
	config.Manager = manager

	manager.Default(true)

	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}

	vts := []avalanche.Vertex{gVtx, mVtx}
	utxos := []ids.ID{ids.GenerateTestID()}

	tx := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	tx.InputIDsV.Add(utxos[0])

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{tx},
		BytesV:   []byte{1, 1, 2, 3},
	}

	manager.EdgeF = func() []ids.ID { return []ids.ID{vts[0].ID(), vts[1].ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		switch {
		case id.Equals(gVtx.ID()):
			return gVtx, nil
		case id.Equals(mVtx.ID()):
			return mVtx, nil
		}
		t.Fatalf("Unknown vertex")
		panic("Should have errored")
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx.Bytes()):
			return vtx, nil
		}
		t.Fatal("asked to parse wrong vertex")
		return nil, errUnknownVertex
	}

	te := &Transitive{}
	te.Initialize(config)
	te.finishBootstrapping()
	te.Ctx.Bootstrapped()

	reqID := new(uint32)
	sender.PushQueryF = func(inVdrs ids.ShortSet, requestID uint32, vtxID ids.ID, _ []byte) {
		*reqID = requestID
		if inVdrs.Len() != 2 {
			t.Fatalf("Wrong number of validators")
		}
		if !vtxID.Equals(vtx.ID()) {
			t.Fatalf("Wrong vertex requested")
		}
	}
	sender.ChitsF = func(vdr ids.ShortID, reqID uint32, _ ids.Set) {
		if !vdr.Equals(vdr0) {
			t.Fatal("should have sent chits to vdr0")
		} else if reqID != 0 {
			t.Fatal("reqID should be 0")
		}
	}

	if err := te.PushQuery(vdr0, 0, vtx.ID(), vtx.Bytes()); err != nil {
		t.Fatal(err)
	}

	votes := ids.Set{}
	votes.Add(vtx.ID())

	if status := tx.Status(); status != choices.Processing {
		t.Fatalf("Wrong tx status: %s ; expected: %s", status, choices.Processing)
	}

	te.Chits(vdr0, *reqID, votes)

	if status := tx.Status(); status != choices.Processing {
		t.Fatalf("Wrong tx status: %s ; expected: %s", status, choices.Processing)
	}

	te.Chits(vdr0, *reqID, votes)

	if status := tx.Status(); status != choices.Processing {
		t.Fatalf("Wrong tx status: %s ; expected: %s", status, choices.Processing)
	}

	manager.SaveVertexF = func(saveVtx avalanche.Vertex) error {
		if saveVtx.ID().Equals(vtx.ID()) {
			return nil
		}
		t.Fatal("saving wrong vertex")
		return errUnknownVertex
	}

	te.Chits(vdr1, *reqID, votes)

	if status := tx.Status(); status != choices.Accepted {
		t.Fatalf("Wrong tx status: %s ; expected: %s", status, choices.Accepted)
	}
}

// Test that we correctly track processing vertices in memory
func TestPinProcessingInMemory(t *testing.T) {
	// Do setup
	config := DefaultConfig()
	vdr := validators.GenerateRandomValidator(1)
	vals := validators.NewSet()
	config.Validators = vals
	vals.AddWeight(vdr.ID(), 1)

	sender := &common.SenderTest{}
	sender.T = t
	config.Sender = sender
	// Key: Vertex ID
	// Value: Request ID of request where we requested chits given this vertex
	pushQueryIDs := map[[32]byte]uint32{}
	sender.PushQueryF = func(_ ids.ShortSet, reqID uint32, vtxID ids.ID, _ []byte) {
		pushQueryIDs[vtxID.Key()] = reqID
	}
	sender.ChitsF = func(ids.ShortID, uint32, ids.Set) {}
	sender.Default(true)

	vm := &vertex.TestVM{}
	vm.T = t
	config.VM = vm

	// Accepted vertices
	gVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	mVtx := &avalanche.TestVertex{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Accepted,
	}}
	vts := []avalanche.Vertex{gVtx, mVtx}

	// The IDs of UTXOs
	utxos := []ids.ID{}
	// Transactions. Element i consumes utxos[i]
	txs := []snowstorm.Tx{}
	for i := 0; i < 20; i++ {
		utxos = append(utxos, ids.GenerateTestID())
		tx := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		}}
		tx.InputIDsV.Add(utxos[i])
		txs = append(txs, tx)
	}

	manager := &vertex.TestManager{T: t}
	config.Manager = manager
	manager.Default(true)
	manager.EdgeF = func() []ids.ID { return []ids.ID{gVtx.ID(), mVtx.ID()} }
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(gVtx.ID()) {
			return gVtx, nil
		} else if id.Equals(mVtx.ID()) {
			return mVtx, nil
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errUnknownVertex
	}

	te := &Transitive{}
	if err := te.Initialize(config); err != nil {
		t.Fatal(err)
	} else if err := te.finishBootstrapping(); err != nil {
		t.Fatal(err)
	}
	te.Ctx.Bootstrapped()

	vtx := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: vts,
		HeightV:  1,
		TxsV:     []snowstorm.Tx{txs[0]},
		BytesV:   []byte{1, 1, 2, 3},
	}

	// First, test the simple case where there's one processing vertex with no conflicts and it's accepted
	if len(te.processing) != 0 {
		t.Fatalf("processing should have %d elements but has %d", 0, len(te.processing))
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		if bytes.Equal(b, vtx.Bytes()) {
			return vtx, nil
		}
		t.Fatal("asked to parse wrong vertex")
		return nil, errUnknownVertex
	}

	te.Put(vdr.ID(), 0, vtx.ID(), vtx.Bytes())
	if len(te.processing) != 1 {
		t.Fatalf("processing should have %d elements but has %d", 1, len(te.processing))
	}

	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		vtxID := vtx.ID()
		switch {
		case vtxID.Equals(vtx.ID()):
			return nil
		}
		t.Fatal("asked to save wrong vertex")
		return errUnknownVertex
	}
	// Record chits
	votes := ids.Set{}
	votes.Add(vtx.ID())
	reqID, ok := pushQueryIDs[vtx.ID().Key()]
	if !ok {
		t.Fatal("should have requested chits for vtx")
	}
	te.Chits(vdr.ID(), reqID, votes)
	if vtx.Status() != choices.Accepted {
		t.Fatal("should be accepted")
	} else if len(te.processing) != 0 {
		t.Fatalf("processing should have %d elements but has %d", 0, len(te.processing))
	}

	// Second, test the case where there are conflicting vertices and vertices with children

	// Add three vertices.
	vtx2 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx},
		TxsV:     []snowstorm.Tx{txs[1]},
		HeightV:  2,
		BytesV:   []byte{2},
	}
	vtx3 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx2},
		TxsV:     []snowstorm.Tx{txs[2]},
		HeightV:  3,
		BytesV:   []byte{3},
	}
	conflictTx := &snowstorm.TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}
	conflictTx.InputIDsV.Add(txs[1].InputIDs().CappedList(1)[0]) // this tx conflicts with txs[1]
	vtx4 := &avalanche.TestVertex{                               // Conflicts with vtx2
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx},
		TxsV:     []snowstorm.Tx{conflictTx},
		HeightV:  2,
		BytesV:   []byte{4},
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx2.Bytes()):
			return vtx2, nil
		case bytes.Equal(b, vtx3.Bytes()):
			return vtx3, nil
		case bytes.Equal(b, vtx4.Bytes()):
			return vtx4, nil
		default:
			t.Fatal("asked for unexpected vertex")
		}
		return nil, errUnknownVertex
	}
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(gVtx.ID()) {
			return gVtx, nil
		} else if id.Equals(mVtx.ID()) {
			return mVtx, nil
		} else if id.Equals(vtx.ID()) {
			return vtx, nil
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errUnknownVertex
	}

	// Issue push queries for the new vertices
	te.PushQuery(vdr.ID(), 3, vtx2.ID(), vtx2.Bytes())
	if len(te.processing) != 1 {
		t.Fatalf("processing should have %d elements but has %d", 1, len(te.processing))
	}
	te.PushQuery(vdr.ID(), 4, vtx3.ID(), vtx3.Bytes())
	if len(te.processing) != 2 {
		t.Fatalf("processing should have %d elements but has %d", 2, len(te.processing))
	}
	te.PushQuery(vdr.ID(), 5, vtx4.ID(), vtx4.Bytes())
	if len(te.processing) != 3 {
		t.Fatalf("processing should have %d elements but has %d", 3, len(te.processing))
	}

	// Current tree:
	//       G
	//       |
	//      vtx
	//     /   \
	//   vtx2   vtx4
	//    |
	//   vtx3

	// Send 2 chits for vtx3. Note that since vtx2 conflicts with vtx4,
	// we need 2 consecutive polls because betaRouge = 2
	votes.Clear()
	votes.Add(vtx3.ID())
	// we could use any two push query request IDs here. We use vtx2 and vtx3's
	reqID, ok = pushQueryIDs[vtx3.ID().Key()]
	if !ok {
		t.Fatal("should have sent push query for vtx3")
	}
	te.Chits(vdr.ID(), reqID, votes)
	reqID, ok = pushQueryIDs[vtx2.ID().Key()]
	if !ok {
		t.Fatal("should have sent push query for vtx2")
	}
	manager.SaveVertexF = func(vtx avalanche.Vertex) error {
		vtxID := vtx.ID()
		switch {
		case vtxID.Equals(vtx2.ID()):
			return nil
		case vtxID.Equals(vtx3.ID()):
			return nil
		}
		t.Fatal("asked to save wrong vertex")
		return errUnknownVertex
	}
	te.Chits(vdr.ID(), reqID, votes)
	// Should have accepted vtx2 and vtx3, rejected vtx4
	if vtx2.Status() != choices.Accepted {
		t.Fatal("should have accepted vtx2")
	} else if vtx3.Status() != choices.Accepted {
		t.Fatal("should have accepted vtx3")
	} else if vtx4.Status() != choices.Rejected {
		t.Fatal("should have rejected vtx4")
	} else if len(te.processing) != 0 {
		t.Fatalf("processing should have %d elements but has %d", 0, len(te.processing))
	}
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(gVtx.ID()) {
			return gVtx, nil
		} else if id.Equals(mVtx.ID()) {
			return mVtx, nil
		} else if id.Equals(vtx.ID()) {
			return vtx, nil
		} else if id.Equals(vtx2.ID()) {
			return vtx2, nil
		} else if id.Equals(vtx3.ID()) {
			return vtx3, nil
		} else if id.Equals(vtx4.ID()) {
			return nil, errUnknownVertex
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errUnknownVertex
	}

	// Third, test the case where a block is immediately dropped because a parent is rejected
	vtx5 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx4}, // vtx4 is rejected so vtx5 will be dropped
		HeightV:  3,
		BytesV:   []byte{5},
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx4.Bytes()):
			vtx4.StatusV = choices.Processing
			conflictTx.StatusV = choices.Processing
			conflictTx.VerifyV = errors.New("utxo alrady consumed")
			vtx4.TxsV[0] = conflictTx
			return vtx4, nil
		case bytes.Equal(b, vtx5.Bytes()):
			return vtx5, nil
		default:
			t.Fatal("asked for unexpected vertex")
		}
		return nil, errUnknownVertex
	}
	sender.GetF = func(_ ids.ShortID, requestID uint32, vtxID ids.ID) {
		reqID = requestID
		if !vtxID.Equals(vtx4.ID()) {
			t.Fatal("asked for wrong vertex")
		}
	}
	te.PushQuery(vdr.ID(), 9, vtx5.ID(), vtx5.Bytes()) // Will call get for vtx4 since we don't have it locally anymore
	te.Put(vdr.ID(), reqID, vtx4.ID(), vtx4.Bytes())
	if len(te.processing) != 0 {
		t.Fatalf("processing should have %d elements but has %d", 0, len(te.processing))
	}

	// Current tree:
	//       G
	//       |
	//      vtx
	//     /   \
	//   vtx2   vtx4 (rej)
	//    |      |
	//   vtx3   vtx5 (rej)

	// Fourth, test another case where a block is dropped
	failTx := &snowstorm.TestTx{ // fails verification
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		VerifyV: errors.New("fail"),
	}
	failTx.InputIDsV.Add(utxos[3])
	vtx6 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Unknown,
		},
		ParentsV: []avalanche.Vertex{vtx3},
		TxsV:     []snowstorm.Tx{failTx},
		HeightV:  4,
		BytesV:   []byte{6},
	}
	vtx7 := &avalanche.TestVertex{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
		ParentsV: []avalanche.Vertex{vtx6},
		HeightV:  5,
		BytesV:   []byte{7},
	}
	manager.GetVertexF = func(id ids.ID) (avalanche.Vertex, error) {
		if id.Equals(gVtx.ID()) {
			return gVtx, nil
		} else if id.Equals(mVtx.ID()) {
			return mVtx, nil
		} else if id.Equals(vtx.ID()) {
			return vtx, nil
		} else if id.Equals(vtx2.ID()) {
			return vtx2, nil
		} else if id.Equals(vtx3.ID()) {
			return vtx3, nil
		} else if id.Equals(vtx6.ID()) {
			return nil, errUnknownVertex
		}
		t.Fatal("asked to get wrong vertex")
		return nil, errUnknownVertex
	}
	manager.ParseVertexF = func(b []byte) (avalanche.Vertex, error) {
		switch {
		case bytes.Equal(b, vtx6.Bytes()):
			return vtx6, nil
		case bytes.Equal(b, vtx7.Bytes()):
			return vtx7, nil
		default:
			t.Fatal("asked for unexpected vertex")
		}
		return nil, errUnknownVertex
	}
	reqID = 0
	requested := false
	sender.GetF = func(vdr ids.ShortID, requestID uint32, vtxID ids.ID) {
		if !vtxID.Equals(vtx6.ID()) {
			t.Fatal("should've asked for vtx6")
		}
		requested = true
		reqID = requestID
	}
	te.PushQuery(vdr.ID(), 10, vtx7.ID(), vtx7.Bytes()) // Will block on vtx6
	if !requested {
		t.Fatal("should have requested vtx6")
	} else if len(te.processing) != 1 {
		t.Fatalf("processing should have %d elements but has %d", 1, len(te.processing))
	}
	te.Put(vdr.ID(), reqID, vtx6.ID(), vtx6.Bytes())

	// Current tree:
	//       G
	//       |
	//      vtx
	//     /   \
	//   vtx2   vtx4 (rej.)
	//    |      |
	//   vtx3   vtx5 (rej.)
	//    |
	//   vtx6
	//    |
	//   vtx7

	// vtx7 becomes unblocked. vtx6 fails verification, so vtx7 gets dropped.
	if len(te.processing) != 0 {
		t.Fatalf("processing should have %d elements but has %d", 0, len(te.processing))
	}
}
