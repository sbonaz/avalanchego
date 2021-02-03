package archive

import (
	"fmt"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/database/prefixdb"
	"github.com/ava-labs/avalanchego/vms/components/state"
)

type Archive interface {
	state.State

	ArchiveDB(database.Database) database.Database
}

// implements Archive
type archive struct {
	state.State
}

func NewArchive() (Archive, error) {
	// TODO: must be prefixed
	rawState, err := state.NewState()
	if err != nil {
		return nil, fmt.Errorf("error creating new state: %w", err)
	}

	return &archive{State: rawState}, nil
}

func (a *archive) ArchiveDB(db database.Database) database.Database {
	// probably a better way to do this
	// We are not closing this soooo
	return prefixdb.New([]byte("archive"), db)
}
