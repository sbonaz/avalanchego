// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package conflicts

import (
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
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
	//
	// Each element must not be rejected
	Dependencies() []Tx

	// PrecludedBy is a set where each element is the ID of a transaction that
	// precludes this transaction. That is, if the transaction is accepted
	// then this transaction must eventually be rejected. Each transaction in
	// this list must have been added to consensus and must not have been
	// accepted when this transaction is added to consensus.
	//
	// Preclusion is not necessarily symmetric.
	// If Tx A precludes Tx B, Tx B need not preclude Tx A.
	//
	// Preclusion is not necessarily transitive.
	// If Tx A precludes Tx B, and Tx B precludes Tx C,
	// Tx A need not preclude Tx C.
	//
	// In the context of a UTXO-based payments system, for example, Tx A
	// would preclude Tx B and vice versa if Tx A and Tx B consume the same UTXO.
	//
	// PrecludedBy is only evaluated once, when the tx is put into consensus.
	// Its return value should never change.
	// If a transaction is added to consensus after this one, and that
	// transaction precludes this one, it should include this transaction's ID
	// in its Precludes() method.
	PrecludedBy() []ids.ID

	// Precludes is a set where each element is the ID of a transaction
	// that this transaction precludes. Each transaction in the set must
	// be processing at the time this transaction is added.
	// Precludes is only evaluated once, when the tx is put into consensus.
	Precludes() []ids.ID

	// Verify that this transaction is valid
	Verify() error

	// Bytes returnes the byte representation of this transaction
	Bytes() []byte
}
