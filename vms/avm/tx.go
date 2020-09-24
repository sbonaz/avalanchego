// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avm

import (
	"fmt"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow"
	"github.com/ava-labs/avalanchego/snow/choices"
	"github.com/ava-labs/avalanchego/utils/codec"
	"github.com/ava-labs/avalanchego/utils/crypto"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/vms/components/avax"
	"github.com/ava-labs/avalanchego/vms/components/verify"
	"github.com/ava-labs/avalanchego/vms/nftfx"
	"github.com/ava-labs/avalanchego/vms/secp256k1fx"
)

// UnsignedTx ...
type UnsignedTx interface {
	Initialize(unsignedBytes, bytes []byte)
	ID() ids.ID
	UnsignedBytes() []byte
	Bytes() []byte

	ConsumedAssetIDs() ids.Set
	AssetIDs() ids.Set

	NumCredentials() int
	InputUTXOs() []*avax.UTXOID
	UTXOs() []*avax.UTXO

	SyntacticVerify(
		ctx *snow.Context,
		c codec.Codec,
		txFeeAssetID ids.ID,
		txFee uint64,
		creationTxFee uint64,
		numFxs int,
	) error
	SemanticVerify(vm *VM, tx UnsignedTx, creds []verify.Verifiable) error
	ExecuteWithSideEffects(vm *VM, batch database.Batch) error
}

// Tx is the core operation that can be performed. The tx uses the UTXO model.
// Specifically, a txs inputs will consume previous txs outputs. A tx will be
// valid if the inputs have the authority to consume the outputs they are
// attempting to consume and the inputs consume sufficient state to produce the
// outputs.
type Tx struct {
	UnsignedTx `serialize:"true" json:"unsignedTx"`
	Creds      []verify.Verifiable `serialize:"true" json:"credentials"` // The credentials of this transaction

	vm                        *VM
	verifiedTx, verifiedState bool
	validity                  error
	inputs                    ids.Set
	inputUTXOs                []*avax.UTXOID
	utxos                     []*avax.UTXO
	deps                      []ids.ID
	status                    choices.Status
}

// Credentials describes the authorization that allows the Inputs to consume the
// specified UTXOs. The returned array should not be modified.
func (t *Tx) Credentials() []verify.Verifiable { return t.Creds }

// SyntacticVerify verifies that this transaction is well-formed.
func (t *Tx) SyntacticVerify(
	ctx *snow.Context,
	c codec.Codec,
	txFeeAssetID ids.ID,
	txFee uint64,
	creationTxFee uint64,
	numFxs int,
) error {
	switch {
	case t == nil || t.UnsignedTx == nil:
		return errNilTx
	case t.verifiedTx:
		return t.validity
	}

	t.verifiedTx = true
	if err := t.UnsignedTx.SyntacticVerify(ctx, c, txFeeAssetID, txFee, creationTxFee, numFxs); err != nil {
		return err
	}

	for _, cred := range t.Creds {
		if err := cred.Verify(); err != nil {
			err := fmt.Errorf("credential failed verification: %w", err)
			t.validity = err
			return err
		}
	}

	if numCreds := t.UnsignedTx.NumCredentials(); numCreds != len(t.Creds) {
		return fmt.Errorf("tx has %d credentials but %d inputs. Should be same",
			len(t.Creds),
			numCreds,
		)
	}
	return nil
}

/*
// SemanticVerify the validity of this transaction
func (t *Tx) SemanticVerify() error {
	if t == nil {
		return errNilTx
	}
	// SyntacticVerify sets the error on validity and is checked in the next
	// statement
	_ = t.SyntacticVerify()

	if t.validity != nil || t.verifiedState {
		return t.validity
	}
	return t.UnsignedTx.SemanticVerify(t.vm, t.UnsignedTx, t.Creds)
}
*/

// SemanticVerify verifies that this transaction is well-formed.
func (t *Tx) SemanticVerify(vm *VM, tx UnsignedTx) error {
	if t == nil {
		return errNilTx
	}

	return t.UnsignedTx.SemanticVerify(vm, tx, t.Creds)
}

// SignSECP256K1Fx ...
func (t *Tx) SignSECP256K1Fx(c codec.Codec, signers [][]*crypto.PrivateKeySECP256K1R) error {
	unsignedBytes, err := c.Marshal(&t.UnsignedTx)
	if err != nil {
		return fmt.Errorf("problem creating transaction: %w", err)
	}

	hash := hashing.ComputeHash256(unsignedBytes)
	for _, keys := range signers {
		cred := &secp256k1fx.Credential{
			Sigs: make([][crypto.SECP256K1RSigLen]byte, len(keys)),
		}
		for i, key := range keys {
			sig, err := key.SignHash(hash)
			if err != nil {
				return fmt.Errorf("problem creating transaction: %w", err)
			}
			copy(cred.Sigs[i][:], sig)
		}
		t.Creds = append(t.Creds, cred)
	}

	signedBytes, err := c.Marshal(t)
	if err != nil {
		return fmt.Errorf("problem creating transaction: %w", err)
	}
	t.Initialize(unsignedBytes, signedBytes)
	return nil
}

// SignNFTFx ...
func (t *Tx) SignNFTFx(c codec.Codec, signers [][]*crypto.PrivateKeySECP256K1R) error {
	unsignedBytes, err := c.Marshal(&t.UnsignedTx)
	if err != nil {
		return fmt.Errorf("problem creating transaction: %w", err)
	}

	hash := hashing.ComputeHash256(unsignedBytes)
	for _, keys := range signers {
		cred := &nftfx.Credential{Credential: secp256k1fx.Credential{
			Sigs: make([][crypto.SECP256K1RSigLen]byte, len(keys)),
		}}
		for i, key := range keys {
			sig, err := key.SignHash(hash)
			if err != nil {
				return fmt.Errorf("problem creating transaction: %w", err)
			}
			copy(cred.Sigs[i][:], sig)
		}
		t.Creds = append(t.Creds, cred)
	}

	signedBytes, err := c.Marshal(t)
	if err != nil {
		return fmt.Errorf("problem creating transaction: %w", err)
	}
	t.Initialize(unsignedBytes, signedBytes)
	return nil
}

// Sets [t]'s status to [status] and persists it in database
func (t *Tx) setStatus(status choices.Status) error {
	if t.status == status {
		return nil
	}
	t.status = status
	return t.vm.state.SetStatus(t.ID(), status)
}

// Status returns the current status of this transaction
func (t *Tx) Status() choices.Status {
	if t.status != choices.Unknown {
		return t.status
	}
	status, err := t.vm.state.Status(t.ID())
	if err != nil {
		return choices.Unknown
	}
	t.status = status
	return status
}

// Accept is called when the transaction was finalized as accepted by consensus
func (t *Tx) Accept() error {
	if s := t.Status(); s != choices.Processing {
		t.vm.ctx.Log.Error("Failed to accept tx %s because the tx is in state %s", t.ID(), s)
		return fmt.Errorf("transaction has invalid status: %s", s)
	}

	defer t.vm.db.Abort()

	// Remove spent utxos
	for _, utxo := range t.InputUTXOs() {
		if utxo.Symbolic() {
			// If the UTXO is symbolic, it can't be spent
			continue
		}
		utxoID := utxo.InputID()
		if err := t.vm.state.SpendUTXO(utxoID); err != nil {
			t.vm.ctx.Log.Error("Failed to spend utxo %s due to %s", utxoID, err)
			return err
		}
	}

	// Add new utxos
	for _, utxo := range t.UTXOs() {
		if err := t.vm.state.FundUTXO(utxo); err != nil {
			t.vm.ctx.Log.Error("Failed to fund utxo %s due to %s", utxo.InputID(), err)
			return err
		}
	}

	if err := t.setStatus(choices.Accepted); err != nil {
		t.vm.ctx.Log.Error("Failed to accept tx %s due to %s", t.ID(), err)
		return err
	}

	txID := t.ID()
	commitBatch, err := t.vm.db.CommitBatch()
	if err != nil {
		t.vm.ctx.Log.Error("Failed to calculate CommitBatch for %s due to %s", txID, err)
		return err
	}

	if err := t.ExecuteWithSideEffects(t.vm, commitBatch); err != nil {
		t.vm.ctx.Log.Error("Failed to commit accept %s due to %s", txID, err)
		return err
	}

	t.vm.ctx.Log.Verbo("Accepted Tx: %s", txID)
	t.vm.pubsub.Publish("accepted", txID)

	delete(t.vm.processingTxs, txID.Key())
	t.deps = nil // Needed to prevent a memory leak

	return nil
}

// Dependencies returns the set of transactions this transaction builds on
func (t *Tx) Dependencies() []ids.ID {
	if len(t.deps) != 0 {
		return t.deps
	}

	txIDs := ids.Set{}
	for _, in := range t.InputUTXOs() {
		if in.Symbolic() {
			continue
		}
		txID, _ := in.InputSource()
		if txIDs.Contains(txID) {
			continue
		}
		txIDs.Add(txID)
		t.deps = append(t.deps, txID)
	}
	consumedIDs := t.ConsumedAssetIDs()
	for _, assetID := range t.AssetIDs().List() {
		if consumedIDs.Contains(assetID) || txIDs.Contains(assetID) {
			continue
		}
		txIDs.Add(assetID)
		t.deps = append(t.deps, assetID)
	}
	return t.deps
}

// Reject is called when the transaction was finalized as rejected by consensus
func (t *Tx) Reject() error {
	defer t.vm.db.Abort()

	if err := t.setStatus(choices.Rejected); err != nil {
		t.vm.ctx.Log.Error("Failed to reject tx %s due to %s", t.ID(), err)
		return err
	}

	txID := t.ID()
	t.vm.ctx.Log.Debug("Rejecting Tx: %s", txID)

	if err := t.vm.db.Commit(); err != nil {
		t.vm.ctx.Log.Error("Failed to commit reject %s due to %s", t.ID(), err)
		return err
	}

	t.vm.pubsub.Publish("rejected", txID)

	t.deps = nil // Needed to prevent a memory leak

	return nil
}

// Verify the validity of this transaction
func (t *Tx) Verify() error {
	if err := t.verifyWithoutCacheWrites(); err != nil {
		return err
	}

	txID := t.ID()
	t.vm.processingTxs[txID.Key()] = t

	t.verifiedState = true
	t.vm.pubsub.Publish("verified", txID)
	return nil
}

func (t *Tx) verifyWithoutCacheWrites() error {
	if t == nil {
		return fmt.Errorf("transaction is nil")
	}
	switch status := t.Status(); status {
	case choices.Unknown:
		return fmt.Errorf("transaction %s is unknown", t.ID())
	case choices.Accepted:
		return nil
	case choices.Rejected:
		return fmt.Errorf("transaction %s is rejected", t.ID())
	default:
		return t.SemanticVerify(t.vm, t.UnsignedTx)
	}
}

// InputIDs returns the set of utxoIDs this transaction consumes
func (t *Tx) InputIDs() ids.Set {
	if t.inputs.Len() != 0 {
		return t.inputs
	}

	for _, utxo := range t.InputUTXOs() {
		t.inputs.Add(utxo.InputID())
	}
	return t.inputs
}

// InputUTXOs returns the utxos that will be consumed on tx acceptance
func (t *Tx) InputUTXOs() []*avax.UTXOID {
	if len(t.inputUTXOs) != 0 {
		return t.inputUTXOs
	}
	t.inputUTXOs = t.UnsignedTx.InputUTXOs()
	return t.inputUTXOs
}

// UTXOs returns the utxos that will be added to the UTXO set on tx acceptance
func (t *Tx) UTXOs() []*avax.UTXO {
	if len(t.utxos) != 0 {
		return t.utxos
	}
	t.utxos = t.UnsignedTx.UTXOs()
	return t.utxos
}
