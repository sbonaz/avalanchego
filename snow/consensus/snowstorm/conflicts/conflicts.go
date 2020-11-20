// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package conflicts

import (
	"errors"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
	"github.com/ava-labs/avalanchego/snow/events"
)

var (
	errInvalidTxType = errors.New("invalid tx type")
)

type Conflicts struct {
	// track the currently processing txs
	// ID --> The processing tx with that ID
	txs map[ids.ID]Tx

	// track which txs a given tx precludes.
	// Processing Tx ID --> IDs of txs that the tx precludes.
	precludes map[ids.ID]ids.Set

	// track which txs preclude a given tx
	// Processing Tx ID --> IDs of txs that preclude this tx
	precludedBy map[ids.ID]ids.Set

	// Processing Tx ID --> IDs of txs that depend on this tx
	dependents map[ids.ID]ids.Set

	// keeps track of whether dependencies have been accepted
	pendingAccept events.Blocker

	// track txs that have been marked as ready to accept
	acceptable []choices.Decidable

	// track txs that have been marked as ready to reject
	rejectable []choices.Decidable

	// IDs of txs in [rejectable]
	rejectableIDs ids.Set
}

func New() *Conflicts {
	return &Conflicts{
		txs:         make(map[ids.ID]Tx),
		precludes:   make(map[ids.ID]ids.Set),
		precludedBy: make(map[ids.ID]ids.Set),
		dependents:  make(map[ids.ID]ids.Set),
	}
}

// Add this tx to the conflict set. If this tx is of the correct type, this tx
// will be added to the set of processing txs. It is assumed this tx wasn't
// already processing.
func (c *Conflicts) Add(txIntf choices.Decidable) error {
	tx, ok := txIntf.(Tx)
	if !ok {
		return errInvalidTxType
	}
	txID := tx.ID()

	// Mark that [tx] is processing
	c.txs[txID] = tx

	// Mark which txs preclude [tx]
	precludedBy := c.precludedBy[txID]
	precludedBy.Add(tx.PrecludedBy()...)
	c.precludedBy[txID] = precludedBy

	for precludor := range precludedBy {
		// Note that [precludor] must be processing
		// due to [tx.PrecludedBy]'s spec
		precludes := c.precludes[precludor]
		precludes.Add(txID)
		c.precludes[precludor] = precludes
	}

	// Mark which txs [tx] precludes
	txPrecludes := c.precludes[txID]
	txPrecludes.Add(tx.Precludes()...)
	c.precludes[txID] = txPrecludes

	for precluded := range txPrecludes {
		// Note that [precluded] must be processing
		// due to [tx.Precludes]' spec
		precludedBy := c.precludedBy[precluded]
		precludedBy.Add(txID)
		c.precludedBy[precluded] = precludedBy
	}

	// Mark which txs [tx] depends on
	for _, dependency := range tx.Dependencies() {
		if dependency.Status() != choices.Accepted {
			// Mark that [tx] depends on [dependency]
			// If the dependency isn't accepted, then it must be processing.
			// This tx should be accepted after this tx is accepted. Note that
			// the dependencies can't already be rejected, because it is assumed
			// that this tx is currently considered valid.
			depID := dependency.ID()
			dependents := c.dependents[depID]
			dependents.Add(txID)
			c.dependents[depID] = dependents
		}
	}
	return nil
}

// IsVirtuous returns false iff a processing tx precludes the given tx.
// It may be called with a tx that isn't processing.
func (c *Conflicts) IsVirtuous(txIntf choices.Decidable) (bool, error) {
	tx, ok := txIntf.(Tx)
	if !ok {
		return false, errInvalidTxType
	}

	for precludor := range c.precludedBy[tx.ID()] {
		if _, ok := c.txs[precludor]; ok {
			// [tx] is precluded by a processing tx
			return false, nil
		}
	}
	return true, nil
}

// PrecludedBy returns the set of processing txs that preclude
// the given tx. This method may be called with a tx that
// isn't processing or one that is processing.
func (c *Conflicts) PrecludedBy(txIntf choices.Decidable) ([]choices.Decidable, error) {
	tx, ok := txIntf.(Tx)
	if !ok {
		return nil, errInvalidTxType
	}

	precludedBy := []choices.Decidable{}
	for precludor := range c.precludedBy[tx.ID()] {
		if precludorTx, ok := c.txs[precludor]; ok { // ignore non-processing txs
			precludedBy = append(precludedBy, precludorTx)
		}
	}
	return precludedBy, nil
}

// Precludes returns the processing transactions that the given tx precludes.
// That is, the transactions that, if the given tx is accepted, must
// eventually be eventually rejected.
func (c *Conflicts) Precludes(txIntf choices.Decidable) ([]choices.Decidable, error) {
	tx, ok := txIntf.(Tx)
	if !ok {
		return nil, errInvalidTxType
	}

	precludes := []choices.Decidable{}
	for precluded := range c.precludedBy[tx.ID()] {
		if precludedTx, ok := c.txs[precluded]; ok { // ignore non-processing txs
			precludes = append(precludes, precludedTx)
		}
	}
	return precludes, nil
}

// Accept notifies this conflict manager that a tx has been conditionally
// accepted. This means that, assuming all the txs this tx depends on are
// accepted, then this tx should be accepted as well.
func (c *Conflicts) Accept(txID ids.ID) {
	tx, exists := c.txs[txID]
	if !exists {
		return
	}

	// Marks [tx] as acceptable once all its dependencies are accepted
	toAccept := &acceptor{
		c:  c,
		tx: tx,
	}
	for _, dependency := range tx.Dependencies() {
		if dependency.Status() != choices.Accepted {
			// If the dependency isn't accepted, then it must be processing.
			// This tx should be accepted after this tx is accepted. Note that
			// the dependencies can't already be rejected, because it is assumed
			// that this tx is currently considered valid.
			toAccept.deps.Add(dependency.ID())
		}
	}
	c.pendingAccept.Register(toAccept)
}

func (c *Conflicts) Updateable() ([]choices.Decidable, []choices.Decidable) {
	acceptable := c.acceptable
	c.acceptable = nil

	// Go through each tx that is about to be accepted
	for _, tx := range acceptable {
		txID := tx.ID()

		// Mark as rejectable each tx that [tx] precludes
		for precluded := range c.precludes[txID] {
			precludedTx, isProcessing := c.txs[precluded]
			if isProcessing && !c.rejectableIDs.Contains(precluded) {
				c.rejectableIDs.Add(precluded)
				c.rejectable = append(c.rejectable, precludedTx)
			}
		}

		// Mark that [tx] is no longer processing
		delete(c.txs, txID)
		delete(c.precludes, txID)
		delete(c.dependents, txID)
		for precludor := range c.precludedBy[txID] {
			precludes := c.precludes[precludor]
			precludes.Remove(txID)
			c.precludes[precludor] = precludes
		}
		delete(c.precludedBy, txID)

		// Notify txs waiting on [tx] to be accepted that it is
		c.pendingAccept.Fulfill(txID)
	}

	rejectable := c.rejectable
	c.rejectable = nil
	c.rejectableIDs.Clear()

	// Go through each tx that is about to be rejected
	for _, tx := range rejectable {
		txID := tx.ID()

		// Notify txs that depend on [tx] that they are rejected
		for dependent := range c.dependents[txID] {
			dependentTx, isProcessing := c.txs[dependent]
			if isProcessing && !c.rejectableIDs.Contains(dependent) {
				c.rejectableIDs.Add(dependent)
				c.rejectable = append(c.rejectable, dependentTx)
			}
		}
		delete(c.dependents, txID)

		// Mark that [tx] is no longer processing
		delete(c.txs, txID)
		delete(c.precludes, txID)
		for precludor := range c.precludedBy[txID] {
			precludes := c.precludes[precludor]
			precludes.Remove(txID)
			c.precludes[precludor] = precludes
		}
		delete(c.precludedBy, txID)

		// Notify txs waiting on [tx] to be accepted that it won't be
		c.pendingAccept.Abandon(txID)
	}

	return acceptable, rejectable
}
