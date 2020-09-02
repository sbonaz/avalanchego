// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package vertex

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ava-labs/gecko/cache"
	"github.com/ava-labs/gecko/database"
	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/consensus/avalanche"
	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
	"github.com/ava-labs/gecko/utils"
	"github.com/ava-labs/gecko/utils/codec"
	"github.com/ava-labs/gecko/utils/hashing"
	"github.com/ava-labs/gecko/utils/math"
	"github.com/ava-labs/gecko/utils/wrappers"
)

var (
	// Key in the database whose value is the edge
	edgeDBKey = ids.Empty.Bytes()
)

// Manager defines the persistant storage that is required by the consensus
// engine
type Manager interface {
	// Create a new vertex from the contents of a vertex
	BuildVertex(parentIDs ids.Set, txs []snowstorm.Tx) (avalanche.Vertex, error)

	// Attempt to convert a stream of bytes into a vertex
	ParseVertex(vertex []byte) (avalanche.Vertex, error)

	// GetVertex attempts to load a vertex by hash from storage
	GetVertex(vtxID ids.ID) (avalanche.Vertex, error)

	// SaveVertex saves a block to persistent storage
	SaveVertex(vtx avalanche.Vertex) error

	// Edge returns a list of accepted vertex IDs with no accepted children
	Edge() (vtxIDs []ids.ID)
}

// ManagerConfig is the config for a Manager
type ManagerConfig struct {
	Ctx          *snow.Context
	ParseTxF     func([]byte) (snowstorm.Tx, error)
	DB           database.Database
	VtxCacheSize int
}

// NewManager returns a new manager that uses [db] for persistence.
func NewManager(config *ManagerConfig) (Manager, error) {
	m := &manager{
		ctx:      config.Ctx,
		parseTxF: config.ParseTxF,
		db:       config.DB,
		vtxCache: cache.LRU{Size: config.VtxCacheSize},
		Codec:    codec.NewDefault(),
	}

	edgeBytes, err := m.db.Get(edgeDBKey)
	if err != nil && err == database.ErrNotFound {
		// We've never used this database before
		// Set edge to empty set
		m.edge = ids.Set{}
		return m, nil
	} else if err != nil {
		// Some other db error has occured
		return nil, fmt.Errorf("couldn't read edge from database: %w", err)
	}

	var edgeList []ids.ID
	if err := m.Codec.Unmarshal(edgeBytes, &edgeList); err != nil {
		return nil, fmt.Errorf("couldn't deserialize edge: %w", err)
	}
	m.edge.Add(edgeList...)
	return m, nil
}

type manager struct {
	// Codec only used to serialize/deserialize edge list
	codec.Codec

	ctx *snow.Context

	db database.Database

	// Parses a tx from bytes
	parseTxF func([]byte) (snowstorm.Tx, error)

	// IDs of accepted vertices with no accepted children
	edge ids.Set

	// Cache of vertices
	// Key: Vertex ID
	// Value: The *vertex
	vtxCache cache.LRU
}

// BuildVertex builds a vertex whose parents are the vertices in [parentIDs], and whose
// transactions are [txs].
func (m *manager) BuildVertex(parentIDs ids.Set, txs []snowstorm.Tx) (avalanche.Vertex, error) {
	if len(txs) == 0 {
		return nil, errNoTxs
	}

	parentIDsList := parentIDs.List()
	ids.SortIDs(parentIDsList)
	sortTxs(txs)

	// Vertex's height is 1 + max height of parents
	height := uint64(0)
	for _, parentID := range parentIDsList {
		parent, err := m.getVertex(parentID)
		if err != nil {
			return nil, err
		}
		height = math.Max64(height, parent.Hght)
	}
	height++

	vtx := &vertex{
		mgr:          m,
		ChainID:      m.ctx.ChainID,
		Hght:         height,
		ParentIDs:    parentIDsList,
		Transactions: txs,
	}
	var err error
	if vtx.bytes, err = vtx.Marshal(); err != nil {
		return nil, fmt.Errorf("couldn't serialize vertex: %w", err)
	}
	vtx.id = ids.NewID(hashing.ComputeHash256Array(vtx.bytes))

	m.vtxCache.Put(vtx.id, vtx)
	return vtx, nil
}

func (m *manager) GetVertex(vtxID ids.ID) (avalanche.Vertex, error) {
	return m.getVertex(vtxID)
}

func (m *manager) getVertex(vtxID ids.ID) (*vertex, error) {
	if vtxIntf, ok := m.vtxCache.Get(vtxID); ok {
		if vtx, ok := vtxIntf.(*vertex); ok {
			return vtx, nil
		}
	}
	vtxBytes, err := m.db.Get(vtxID.Bytes())
	if err != nil {
		return nil, fmt.Errorf("couldn't find vertex %s", vtxID)
	}
	vtx, err := m.parseVertex(vtxBytes)
	if err != nil {
		return nil, fmt.Errorf("couldn't deserialize to vertex %s: %w", vtxID, err)
	}
	m.vtxCache.Put(vtx.id, &vtx)
	return vtx, nil
}

func (m *manager) SaveVertex(vtx avalanche.Vertex) error {
	return m.db.Put(vtx.ID().Bytes(), vtx.Bytes())
}

// saveEdge persists the current edge to the database
func (m *manager) saveEdge() error {
	edgeBytes, err := m.Codec.Marshal(m.edge.List())
	if err != nil {
		return fmt.Errorf("couldn't serialize edge: %w", err)
	}
	return m.db.Put(edgeDBKey, edgeBytes)
}

// Edge returns a list of accepted vertex IDs with no accepted children
func (m *manager) Edge() []ids.ID {
	return m.edge.List()
}

// Unmarshal attempts to parse a vertex from bytes.
func (m *manager) ParseVertex(b []byte) (avalanche.Vertex, error) {
	return m.parseVertex(b)
}

// Unmarshal attempts to parse a vertex from bytes.
func (m *manager) parseVertex(b []byte) (*vertex, error) {
	p := wrappers.Packer{Bytes: b}

	if codecID := p.UnpackShort(); codecID != codecVersion {
		p.Add(fmt.Errorf("expected codec version %d but got %d", codecVersion, codecID))
	}

	chainID, _ := ids.ToID(p.UnpackFixedBytes(hashing.HashLen))
	height := p.UnpackLong()
	if gotEpoch := p.UnpackInt(); gotEpoch != 0 {
		p.Add(fmt.Errorf("expected epoch %d but got %d", epoch, gotEpoch))
	}

	parentIDs := []ids.ID(nil)
	for i := p.UnpackInt(); i > 0 && !p.Errored(); i-- {
		parentID, _ := ids.ToID(p.UnpackFixedBytes(hashing.HashLen))
		parentIDs = append(parentIDs, parentID)
	}

	txs := []snowstorm.Tx(nil)
	for i := p.UnpackInt(); i > 0 && !p.Errored(); i-- {
		tx, err := m.parseTxF(p.UnpackBytes())
		p.Add(err)
		txs = append(txs, tx)
	}

	if p.Offset != len(b) {
		p.Add(fmt.Errorf("%d unused bytes after unmarshalling", len(b)-p.Offset))
	}

	if p.Errored() {
		return nil, p.Err
	}

	vtx := &vertex{
		mgr:          m,
		id:           ids.NewID(hashing.ComputeHash256Array(b)),
		ParentIDs:    parentIDs,
		ChainID:      chainID,
		Hght:         height,
		Transactions: txs,
		bytes:        b,
	}
	m.vtxCache.Put(vtx.id, vtx)
	return vtx, nil
}

func sortTxs(txs []snowstorm.Tx) { sort.Sort(sortTxsData(txs)) }

func isSortedAndUniqueTxs(txs []snowstorm.Tx) bool {
	return utils.IsSortedAndUnique(sortTxsData(txs))
}

type sortTxsData []snowstorm.Tx

func (txs sortTxsData) Less(i, j int) bool {
	return bytes.Compare(txs[i].ID().Bytes(), txs[j].ID().Bytes()) == -1
}
func (txs sortTxsData) Len() int      { return len(txs) }
func (txs sortTxsData) Swap(i, j int) { txs[j], txs[i] = txs[i], txs[j] }
