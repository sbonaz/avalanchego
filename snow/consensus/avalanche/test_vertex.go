// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avalanche

import (
	"github.com/ava-labs/avalanche-go/ids"
	"github.com/ava-labs/avalanche-go/snow/choices"
	"github.com/ava-labs/avalanche-go/snow/consensus/snowstorm"
)

// TestVertex is a useful test vertex
type TestVertex struct {
	choices.TestDecidable

	ParentsV    []Vertex
	ParentsErrV error
	HeightV     uint64
	HeightErrV  error
	TxsV        []snowstorm.Tx
	TxsErrV     error
	BytesV      []byte
}

// Parents implements the Vertex interface
func (v *TestVertex) Parents() ([]ids.ID, error) {
	ids := make([]ids.ID, len(v.ParentsV))
	for i, parent := range v.ParentsV {
		ids[i] = parent.ID()
	}
	return ids, v.ParentsErrV
}

// Height implements the Vertex interface
func (v *TestVertex) Height() (uint64, error) { return v.HeightV, v.HeightErrV }

// Txs implements the Vertex interface
func (v *TestVertex) Txs() ([]snowstorm.Tx, error) { return v.TxsV, v.TxsErrV }

// Bytes implements the Vertex interface
func (v *TestVertex) Bytes() []byte { return v.BytesV }

// TestVertexGetter the VertexGetter interface
type testVertexGetter struct {
	GetVertexF func(ids.ID) (Vertex, error)
}

// GetVertex ...
func (g *testVertexGetter) GetVertex(id ids.ID) (Vertex, error) {
	if g.GetVertexF == nil {
		panic("unexpectedly called GetVertex")
	}
	return g.GetVertexF(id)
}
