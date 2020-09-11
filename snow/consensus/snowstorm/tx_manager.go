package snowstorm

import "github.com/ava-labs/avalanchego/ids"

// TxManager stores and retrieves transactions
type TxManager interface {
	// Get a transaction by its ID
	GetTx(ids.ID) (Tx, error)

	// Persist a transaction to storage
	SaveTx(Tx) error
}
