package vertex

import (
	"errors"
	"fmt"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow/choices"
	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
	"github.com/ava-labs/gecko/utils/wrappers"
)

const (
	// Maximum size of a vertex in bytes.
	maxVertexSize = 1 << 20
	epoch         = 0 // The current epoch. Alwyas 0 for now.
	codecVersion  = uint16(0)
)

var (
	errInvalidParents = errors.New("parent IDs are not unique and sorted")
	errNoTxs          = errors.New("vertex has no transactions")
	errInvalidTxs     = errors.New("transactions aren't sorted or aren't unique")
	errConflictingTxs = errors.New("vertex contains conflicting transactions")
)

// A vertex
type vertex struct {
	// The vertex manager this vertex lives inside
	mgr *manager

	// Hash of byte representation of this vertex
	// Should be set on creation/deserialization
	id ids.ID

	// The status of this vertex
	status choices.Status

	// ID of the chain this vertex exists on
	ChainID ids.ID `serialize:"true"`

	// The maximum height of a parent + 1
	Hght uint64 `serialize:"true"`

	// The epoch this vertex exists in
	Epoch uint32 `serialize:"true"`

	// IDs of parent vertices
	ParentIDs []ids.ID `serialize:"true"`

	// Transactions this vertex contains
	Transactions []snowstorm.Tx `serialize:"true"`

	// Byte representation of this vertex
	// Should be set on creation/deserialization
	bytes []byte
}

// ID returns this vertex's ID.
// Fulfills the avalanche.Vertex interface.
func (vtx *vertex) ID() ids.ID { return vtx.id }

// Height returns this vertex's height, which is 1 + the max height of its parents.
// Fulfills the avalanche.Vertex interface.
func (vtx *vertex) Height() (uint64, error) { return vtx.Hght, nil }

// Bytes returns the byte representation of this vertex.
func (vtx *vertex) Bytes() []byte { return vtx.bytes }

// Status returns this vertex's status.
// Fulfills the avalanche.Vertex interface.
func (vtx *vertex) Status() choices.Status { return vtx.status }

// Txs returns the transactions in this vertex.
// Fulfills the avalanche.Vertex interface.
func (vtx *vertex) Txs() ([]snowstorm.Tx, error) {
	return vtx.Transactions, nil
}

// Parents returns the IDs of this vertex's parents.
// Fulfills the avalanche.Vertex interface.
func (vtx *vertex) Parents() ([]ids.ID, error) {
	return vtx.ParentIDs, nil
}

// Reject this transaction.
// Fulfills the avalanche.Vertex interface.
func (vtx *vertex) Reject() error {
	vtx.status = choices.Rejected
	return nil
}

func (vtx *vertex) Verify() error {
	switch {
	case !ids.IsSortedAndUniqueIDs(vtx.ParentIDs):
		return errInvalidParents
	case len(vtx.Transactions) == 0:
		return errNoTxs
	case !isSortedAndUniqueTxs(vtx.Transactions):
		return errInvalidTxs
	}

	inputIDs := ids.Set{}
	for _, tx := range vtx.Transactions {
		inputs := tx.InputIDs()
		if inputs.Overlaps(inputIDs) {
			return errConflictingTxs
		}
		inputIDs.Union(inputs)
	}

	return nil
}

func (vtx *vertex) Accept() error {
	vtx.status = choices.Accepted

	for _, parentID := range vtx.ParentIDs {
		vtx.mgr.edge.Remove(parentID)
	}
	vtx.mgr.edge.Add(vtx.ID())

	if err := vtx.mgr.saveEdge(); err != nil {
		return fmt.Errorf("couldn't save edge: %w", err)
	}
	return nil
}

// Marshal creates the byte representation of the vertex
func (vtx *vertex) Marshal() ([]byte, error) {
	p := wrappers.Packer{MaxSize: maxVertexSize}

	p.PackShort(codecVersion)
	p.PackFixedBytes(vtx.ChainID.Bytes())
	p.PackLong(vtx.Hght)
	p.PackInt(epoch)

	p.PackInt(uint32(len(vtx.ParentIDs)))
	for _, parentID := range vtx.ParentIDs {
		p.PackFixedBytes(parentID.Bytes())
	}

	p.PackInt(uint32(len(vtx.Transactions)))
	for _, tx := range vtx.Transactions {
		p.PackBytes(tx.Bytes())
	}
	return p.Bytes, p.Err
}
