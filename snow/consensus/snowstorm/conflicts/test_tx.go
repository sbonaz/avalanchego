// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package conflicts

import (
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
)

// TestTx is a useful test tx
type TestTx struct {
	choices.TestDecidable
	DependenciesV []Tx
	PrecludedByV  []ids.ID
	PrecludesV    []ids.ID
	VerifyV       error
	BytesV        []byte
}

// Dependencies implements the Tx interface
func (t *TestTx) Dependencies() []Tx { return t.DependenciesV }

// PrecludedBy implements the Tx interface
func (t *TestTx) PrecludedBy() []ids.ID { return t.PrecludedByV }

// Precludes implements the Tx interface
func (t *TestTx) Precludes() []ids.ID { return t.PrecludesV }

// Verify implements the Tx interface
func (t *TestTx) Verify() error { return t.VerifyV }

// Bytes implements the Tx interface
func (t *TestTx) Bytes() []byte { return t.BytesV }
