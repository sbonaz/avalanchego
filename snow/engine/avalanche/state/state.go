// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package state

import (
	"fmt"

	"github.com/ava-labs/avalanchego/cache"
	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
	"github.com/ava-labs/avalanchego/utils/formatting"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/wrappers"
)

type state struct {
	serializer *Serializer

	dbCache cache.Cacher
	db      database.Database
}

func (s *state) Vertex(id ids.ID) *innerVertex {
	if vtxIntf, found := s.dbCache.Get(id); found {
		vtx, ok := vtxIntf.(*innerVertex)
		if ok {
			return vtx
		} else if vtxIntf != nil {
			s.serializer.ctx.Log.Error("got unexpected type %T in cache for vertex %s", vtxIntf, id)
		}
	}

	bytes, err := s.db.Get(id.Bytes())
	if err == nil {
		// The key was in the database
		if vtx, err := s.serializer.parseVertex(bytes); err == nil {
			s.dbCache.Put(id, vtx) // Cache the element
			return vtx
		}
		s.serializer.ctx.Log.Error("Parsing failed on saved vertex.\nPrefixed key = %s\nBytes = %s",
			id,
			formatting.DumpBytes{Bytes: bytes})
	} else if err != database.ErrNotFound {
		s.serializer.ctx.Log.Error("db error while getting vertex %s: %s", id, err)
	}

	s.dbCache.Put(id, nil) // Cache the miss
	return nil
}

// SetVertex persists the vertex to the database and returns an error if it
// fails to write to the db
func (s *state) SetVertex(id ids.ID, vtx *innerVertex) error {
	s.dbCache.Put(id, vtx)

	if vtx == nil {
		return s.db.Delete(id.Bytes())
	}

	return s.db.Put(id.Bytes(), vtx.bytes)
}

func (s *state) Status(id ids.ID) choices.Status {
	if statusIntf, found := s.dbCache.Get(id); found {
		if status, ok := statusIntf.(choices.Status); ok {
			return status
		} else if statusIntf != nil {
			s.serializer.ctx.Log.Error("got unexpected type %T in cache for status %s", statusIntf, id)
		}
	}

	bytes, err := s.db.Get(id.Bytes())
	if err == nil {
		// The key was in the database
		p := wrappers.Packer{Bytes: bytes}
		status := choices.Status(p.UnpackInt())
		if p.Offset == len(bytes) && !p.Errored() {
			s.dbCache.Put(id, status)
			return status
		}
		s.serializer.ctx.Log.Error("Parsing failed on saved status.\nPrefixed key = %s\nBytes = \n%s",
			id,
			formatting.DumpBytes{Bytes: bytes})
	}

	s.dbCache.Put(id, choices.Unknown)
	return choices.Unknown
}

// SetStatus sets the status of the vertex and returns an error if it fails to write to the db
func (s *state) SetStatus(id ids.ID, status choices.Status) error {
	s.dbCache.Put(id, status)

	if status == choices.Unknown {
		return s.db.Delete(id.Bytes())
	}

	p := wrappers.Packer{Bytes: make([]byte, 4)}

	p.PackInt(uint32(status))

	s.serializer.ctx.Log.AssertNoError(p.Err)
	s.serializer.ctx.Log.AssertTrue(p.Offset == len(p.Bytes), "Wrong offset after packing")

	return s.db.Put(id.Bytes(), p.Bytes)
}

// Returns the accepted frontier
// Only returns a non-nil error if an invalid data in the database
func (s *state) Edge(id ids.ID) ([]ids.ID, error) {
	if frontierIntf, found := s.dbCache.Get(id); found {
		if frontier, ok := frontierIntf.([]ids.ID); ok {
			return frontier, nil
		} else if frontierIntf != nil {
			s.serializer.ctx.Log.Error("got unexpected type %T in cache for frontier", frontierIntf)
		}
	}

	bytes, err := s.db.Get(id.Bytes())
	if err == nil {
		p := wrappers.Packer{Bytes: bytes}

		frontier := []ids.ID{}
		for i := p.UnpackInt(); i > 0 && !p.Errored(); i-- {
			id, err := ids.ToID(p.UnpackFixedBytes(hashing.HashLen))
			if err != nil {
				return nil, fmt.Errorf("couldn't parse ID: %w", err)
			}
			frontier = append(frontier, id)
		}

		if p.Offset == len(bytes) && !p.Errored() {
			s.dbCache.Put(id, frontier)
			return frontier, nil
		}
		s.serializer.ctx.Log.Error("Parsing failed on saved ids.\nPrefixed key = %s\nBytes = %s",
			id,
			formatting.DumpBytes{Bytes: bytes})
	} else if err != database.ErrNotFound {
		s.serializer.ctx.Log.Error("db error while getting accepted frontier: %s", err)
	}

	s.dbCache.Put(id, nil) // Cache the miss
	return nil, nil
}

// SetEdge sets the frontier and returns an error if it fails to write to the db
func (s *state) SetEdge(id ids.ID, frontier []ids.ID) error {
	s.dbCache.Put(id, frontier)

	if len(frontier) == 0 {
		return s.db.Delete(id.Bytes())
	}

	size := wrappers.IntLen + hashing.HashLen*len(frontier)
	p := wrappers.Packer{Bytes: make([]byte, size)}

	p.PackInt(uint32(len(frontier)))
	for _, id := range frontier {
		p.PackFixedBytes(id.Bytes())
	}

	s.serializer.ctx.Log.AssertNoError(p.Err)
	s.serializer.ctx.Log.AssertTrue(p.Offset == len(p.Bytes), "Wrong offset after packing")

	return s.db.Put(id.Bytes(), p.Bytes)
}
