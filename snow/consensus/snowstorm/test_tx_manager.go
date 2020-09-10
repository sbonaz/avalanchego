package snowstorm

import (
	"errors"
	"testing"

	"github.com/ava-labs/avalanche-go/ids"
)

var (
	errSaveTx = errors.New("unexpectedly called SaveTx")
	errGetTx  = errors.New("unexpectedly called GetTx")
)

// TestTxManager ...
type TestTxManager struct {
	t                     *testing.T
	CantGetTx, CantSaveTx bool
	GetTxF                func(ids.ID) (Tx, error)
	SaveTxF               func(Tx) error
}

// Default ...
func (vm *TestTxManager) Default(cant bool) {
	vm.CantGetTx = cant
	vm.CantSaveTx = cant
}

// GetTx ...
func (vm *TestTxManager) GetTx(txID ids.ID) (Tx, error) {
	if vm.GetTxF != nil {
		return vm.GetTxF(txID)
	}
	if vm.CantGetTx && vm.t != nil {
		vm.t.Fatal(errGetTx)
	}
	return nil, errGetTx
}

// SaveTx ...
func (vm *TestTxManager) SaveTx(tx Tx) error {
	if vm.SaveTxF != nil {
		return vm.SaveTxF(tx)
	}
	if vm.CantSaveTx && vm.t != nil {
		vm.t.Fatal(errSaveTx)
	}
	return errSaveTx
}
