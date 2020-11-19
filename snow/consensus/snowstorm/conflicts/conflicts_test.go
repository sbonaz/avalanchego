// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package conflicts

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
)

func TestInvalidTx(t *testing.T) {
	c := New()

	tx := &choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}

	{
		err := c.Add(tx)
		assert.Error(t, err)
	}
	{
		_, err := c.IsVirtuous(tx)
		assert.Error(t, err)
	}
	{
		_, err := c.PrecludedBy(tx)
		assert.Error(t, err)
	}
	assert.Empty(t, c.txs)
	assert.Empty(t, c.precludedBy)
	assert.Empty(t, c.precludes)
	assert.Empty(t, c.pendingAccept)
}

func TestPrecludedBy(t *testing.T) {
	type step struct {
		*TestTx
		ExpectedPrecludedBy []*TestTx
		ExpectedToPreclude  []*TestTx
	}

	type test struct {
		txs                 []*TestTx
		expectedPrecludedBy map[ids.ID][]ids.ID
	}

	id1 := ids.GenerateTestID()
	id2 := ids.GenerateTestID()

	tests := []test{
		{
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{id1: nil},
		},
		{
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				},
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id1},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: {id1},
			},
		},
		{
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				},
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV: []ids.ID{id1},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: {id2},
				id2: nil,
			},
		},
		{
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				},
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: {id2},
				id2: {id1},
			},
		},
	}

	for _, test := range tests {
		c := New()

		txIDToTxs := map[ids.ID]*TestTx{}

		// Add the txs
		for _, tx := range test.txs {
			err := c.Add(tx)
			assert.NoError(t, err)
			txIDToTxs[tx.ID()] = tx
		}

		// Ensure that txs preclude each other correctly
		for id, expectedPrecludedBy := range test.expectedPrecludedBy {
			precludedBy, err := c.PrecludedBy(txIDToTxs[id])
			assert.NoError(t, err)
			assert.Len(t, expectedPrecludedBy, len(precludedBy))

			for _, id := range expectedPrecludedBy {
				assert.Contains(t, precludedBy, txIDToTxs[id])
			}
		}

	}
}

func TestIsVirtuousNoConflicts(t *testing.T) {
	c := New()

	tx := &TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}

	virtuous, err := c.IsVirtuous(tx)
	assert.NoError(t, err)
	assert.True(t, virtuous)
}

func TestAcceptConflicts(t *testing.T) {
	c := New()

	tx := &TestTx{TestDecidable: choices.TestDecidable{
		IDV:     ids.GenerateTestID(),
		StatusV: choices.Processing,
	}}

	err := c.Add(tx)
	assert.NoError(t, err)

	toAccepts, toRejects := c.Updateable()
	assert.Empty(t, toAccepts)
	assert.Empty(t, toRejects)

	c.Accept(tx.ID())

	toAccepts, toRejects = c.Updateable()
	assert.Len(t, toAccepts, 1)
	assert.Empty(t, toRejects)
	assert.Empty(t, c.txs)
	assert.Empty(t, c.precludedBy)
	assert.Empty(t, c.precludes)
	assert.Empty(t, c.pendingAccept)

	toAccept := toAccepts[0]
	assert.Equal(t, tx.ID(), toAccept.ID())
}
