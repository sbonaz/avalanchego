// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package queue

import (
	"github.com/ava-labs/avalanchego/cache"
	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/wrappers"
)

// Constants
const (
	stackSizeID byte = iota
	stackID
	jobID
	blockingID
)

var (
	stackSize = []byte{stackSizeID}
)

type prefixedState struct {
	state

	stackSizeSet bool // true if we can use cached [stackSize]
	stackSize    uint32

	// Key: Index
	// Value: Job at that index
	stackIndexCache *cache.LRU

	// Key: Job ID
	// Value: Job
	jobCache *cache.LRU
}

func (ps *prefixedState) SetStackSize(db database.Database, size uint32) error {
	if err := ps.state.SetInt(db, stackSize, size); err != nil {
		return err
	}
	ps.stackSizeSet = true
	ps.stackSize = size
	return nil
}

func (ps *prefixedState) StackSize(db database.Database) (uint32, error) {
	if ps.stackSizeSet {
		return ps.stackSize, nil
	}
	return ps.state.Int(db, stackSize)
}

func (ps *prefixedState) SetStackIndex(db database.Database, index uint32, job Job) error {
	p := wrappers.Packer{Bytes: make([]byte, 1+wrappers.IntLen)}
	p.PackByte(stackID)
	p.PackInt(index)

	if err := ps.state.SetJob(db, p.Bytes, job); err != nil {
		return err
	}
	key := ids.Empty.Prefix(uint64(index))
	ps.stackIndexCache.Put(key, job)
	return nil
}

func (ps *prefixedState) DeleteStackIndex(db database.Database, index uint32) error {
	p := wrappers.Packer{Bytes: make([]byte, 1+wrappers.IntLen)}
	p.PackByte(stackID)
	p.PackInt(index)

	if err := db.Delete(p.Bytes); err != nil {
		return err
	}
	key := ids.Empty.Prefix(uint64(index))
	ps.stackIndexCache.Evict(key)
	return nil
}

func (ps *prefixedState) StackIndex(db database.Database, index uint32) (Job, error) {
	key := ids.Empty.Prefix(uint64(index))
	if jobIntf, ok := ps.stackIndexCache.Get(key); ok {
		return jobIntf.(Job), nil
	}

	p := wrappers.Packer{Bytes: make([]byte, 1+wrappers.IntLen)}
	p.PackByte(stackID)
	p.PackInt(index)

	job, err := ps.state.Job(db, p.Bytes)
	if err != nil {
		return nil, err
	}
	ps.stackIndexCache.Put(key, job)
	return job, nil
}

func (ps *prefixedState) SetJob(db database.Database, job Job) error {
	p := wrappers.Packer{Bytes: make([]byte, 1+hashing.HashLen)}
	p.PackByte(jobID)
	p.PackFixedBytes(job.ID().Bytes())

	if err := ps.state.SetJob(db, p.Bytes, job); err != nil {
		return err
	}
	ps.jobCache.Put(job.ID(), job)
	return nil
}

func (ps *prefixedState) HasJob(db database.Database, id ids.ID) (bool, error) {
	if _, has := ps.jobCache.Get(id); has {
		return true, nil
	}

	p := wrappers.Packer{Bytes: make([]byte, 1+hashing.HashLen)}
	p.PackByte(jobID)
	p.PackFixedBytes(id.Bytes())

	return db.Has(p.Bytes)
}

func (ps *prefixedState) DeleteJob(db database.Database, id ids.ID) error {
	p := wrappers.Packer{Bytes: make([]byte, 1+hashing.HashLen)}
	p.PackByte(jobID)
	p.PackFixedBytes(id.Bytes())

	if err := db.Delete(p.Bytes); err != nil {
		return err
	}
	ps.jobCache.Evict(id)
	return nil
}

func (ps *prefixedState) Job(db database.Database, id ids.ID) (Job, error) {
	if job, ok := ps.jobCache.Get(id); ok {
		return job.(Job), nil
	}

	p := wrappers.Packer{Bytes: make([]byte, 1+hashing.HashLen)}
	p.PackByte(jobID)
	p.PackFixedBytes(id.Bytes())

	job, err := ps.state.Job(db, p.Bytes)
	if err != nil {
		return nil, err
	}
	return job, nil
}

func (ps *prefixedState) AddBlocking(db database.Database, id ids.ID, blocking ids.ID) error {
	p := wrappers.Packer{Bytes: make([]byte, 1+hashing.HashLen)}

	p.PackByte(blockingID)
	p.PackFixedBytes(id.Bytes())

	return ps.state.AddID(db, p.Bytes, blocking)
}

func (ps *prefixedState) DeleteBlocking(db database.Database, id ids.ID, blocking []ids.ID) error {
	p := wrappers.Packer{Bytes: make([]byte, 1+hashing.HashLen)}

	p.PackByte(blockingID)
	p.PackFixedBytes(id.Bytes())

	for _, blocked := range blocking {
		if err := ps.state.RemoveID(db, p.Bytes, blocked); err != nil {
			return err
		}
	}

	return nil
}

func (ps *prefixedState) Blocking(db database.Database, id ids.ID) ([]ids.ID, error) {
	p := wrappers.Packer{Bytes: make([]byte, 1+hashing.HashLen)}

	p.PackByte(blockingID)
	p.PackFixedBytes(id.Bytes())

	return ps.state.IDs(db, p.Bytes)
}
