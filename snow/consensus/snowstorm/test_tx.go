// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package snowstorm

import (
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
)

// TestTx is a useful test tx
type TestTx struct {
	choices.TestDecidable

	DependenciesV []Tx
	InputIDsV     ids.Set
	VerifyV       error
	BytesV        []byte
}

// Dependencies implements the Tx interface
func (t *TestTx) Dependencies() []ids.ID {
	deps := make([]ids.ID, len(t.DependenciesV))
	for i, dep := range t.DependenciesV {
		deps[i] = dep.ID()
	}
	return deps
}

// InputIDs implements the Tx interface
func (t *TestTx) InputIDs() ids.Set { return t.InputIDsV }

// Verify implements the Tx interface
func (t *TestTx) Verify() error { return t.VerifyV }

// Bytes returns the bits
func (t *TestTx) Bytes() []byte { return t.BytesV }
