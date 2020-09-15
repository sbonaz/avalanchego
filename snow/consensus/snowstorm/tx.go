// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package snowstorm

import (
	"errors"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
)

var (
	errCantTopologicalSort = errors.New("topological sort failed on the given set of transactions. It likely has a dependency cycle")
)

// Tx consumes state.
type Tx interface {
	choices.Decidable

	// Dependencies is a list of transactions upon which this transaction
	// depends. Each element of Dependencies must be verified before Verify is
	// called on this transaction.
	//
	// Similarly, each element of Dependencies must be accepted before this
	// transaction is accepted.
	Dependencies() []ids.ID

	// InputIDs is a set where each element is the ID of a piece of state that
	// will be consumed if this transaction is accepted.
	//
	// In the context of a UTXO-based payments system, for example, this would
	// be the IDs of the UTXOs consumed by this transaction
	InputIDs() ids.Set

	// Verify that the state transition this transaction would make if it were
	// accepted is valid. If the state transition is invalid, a non-nil error
	// should be returned.
	//
	// It is guaranteed that when Verify is called, all the dependencies of
	// this transaction have already been successfully verified.
	Verify() error

	// Bytes returns the binary representation of this transaction.
	//
	// This is used for sending transactions to peers. Another node should be
	// able to parse these bytes to the same transaction.
	Bytes() []byte
}

// TopologicalSort a set of transactions using Kahn's algorithm
func TopologicalSort(txs []Tx) ([]Tx, error) {
	txIDs := ids.Set{} // Set containing IDs of [txs]
	for _, tx := range txs {
		txIDs.Add(tx.ID())
	}

	// [txs] sorted in topological order
	sorted := []Tx{}
	// Set of txs with no dependencies in [txs]
	noDeps := []Tx{}

	// Tx ID --> Tx
	txIDToTx := map[[32]byte]Tx{}
	txToDeps := map[[32]byte]ids.Set{}
	for _, tx := range txs {
		key := tx.ID().Key()
		txIDToTx[key] = tx

		deps := tx.Dependencies()
		depsInTxs := ids.Set{} // Dependencies that are in [txs]
		for _, dep := range deps {
			if txIDs.Contains(dep) {
				depsInTxs.Add(dep)
			}
		}
		if depsInTxs.Len() == 0 {
			noDeps = append(noDeps, tx)
		} else {
			txToDeps[key] = depsInTxs
		}
	}

	var tx Tx
	for len(noDeps) != 0 {
		tx, noDeps = noDeps[0], noDeps[1:]
		sorted = append(sorted, tx)
		for txID, deps := range txToDeps {
			deps.Remove(tx.ID())
			if deps.Len() == 0 {
				noDeps = append(noDeps, txIDToTx[txID])
				delete(txToDeps, txID)
			}
		}
	}

	if len(sorted) != len(txs) {
		return nil, errCantTopologicalSort
	}

	return sorted, nil
}
