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
	type accept struct {
		toConditionallyAccept []ids.ID
		expectedAccepted      []ids.ID
		expectedRejected      []ids.ID
	}

	// The test flow is: add all of [txs], conditionally accept some,
	// check that after each conditional acceptance the conflict manager
	// correctly reports which are acceptable/rejectable, then check
	// that the preclusions are as expected
	type test struct {
		name                string
		txs                 []*TestTx
		accepts             []accept
		expectedPrecludedBy map[ids.ID][]ids.ID
	}

	id1 := ids.GenerateTestID()
	id2 := ids.GenerateTestID()
	id3 := ids.GenerateTestID()
	id4 := ids.GenerateTestID()

	tests := []test{
		{
			name: "One transaction; no preclusions; no deps; no accepts",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				},
			},
			accepts:             nil,
			expectedPrecludedBy: map[ids.ID][]ids.ID{id1: nil},
		}, {
			name: "One transaction; no preclusions; no deps; accept",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{id1: nil},
		}, {
			name: "tx1/tx2 unrelated; no deps; accept tx1",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx2 depends on tx1; accept tx1 then tx2",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					dependenciesIDsV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
				}, {
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx2 precluded by tx1; no deps; no accepts",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id1},
				},
			},
			accepts: nil,
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: {id1},
			},
		}, {
			name: "tx2 precluded by tx1; no deps; tx1 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
					expectedRejected:      []ids.ID{id2},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx2 precluded by tx1; no deps; tx2 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      nil,
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx2 precluded by tx1; no deps; tx2 accepted then tx1 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      nil,
				}, {
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
					expectedRejected:      nil,
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx2 precludes tx1; no deps; tx1 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
					expectedRejected:      nil,
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx2 precludes tx1; no deps; tx1 accepted then tx2 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
					expectedRejected:      nil,
				}, {
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      nil,
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx2 precludes tx1; no deps; tx2 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      []ids.ID{id1},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; no accepts",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				},
			},
			accepts: nil,
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: {id2},
				id2: {id1},
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx1 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
					expectedRejected:      []ids.ID{id2},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx2 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      []ids.ID{id1},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx3 unrelated; no accepts",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
				},
			},
			accepts: nil,
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: {id2},
				id2: {id1},
				id3: nil,
			},
		}, {
			name: "tx1 precludes tx2; tx3 depends on tx2; tx4 unrelated; accept tx1",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
					dependenciesIDsV: []ids.ID{id2},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id4,
						StatusV: choices.Processing,
					},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
					expectedRejected:      []ids.ID{id2},
				}, {
					toConditionallyAccept: []ids.ID{id4},
					expectedAccepted:      []ids.ID{id4},
					expectedRejected:      []ids.ID{id3},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
				id3: nil,
				id4: nil,
			},
		}, {
			name: "tx2 and tx3 depend on tx1; tx2/tx3 mutually exclusive; accept tx1",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					dependenciesIDsV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
					dependenciesIDsV: []ids.ID{id1},
					PrecludedByV:     []ids.ID{id2},
					PrecludesV:       []ids.ID{id2},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
				}, {
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      []ids.ID{id3},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
				id3: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx3 unrelated; tx3 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
				},
			},
			accepts: nil,
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: {id2},
				id2: {id1},
				id3: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx3 unrelated; tx3 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id3},
					expectedAccepted:      []ids.ID{id3},
					expectedRejected:      nil,
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: {id2},
				id2: {id1},
				id3: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx3 unrelated; tx2 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      []ids.ID{id1},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
				id3: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx3 precludes tx2; tx3 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
					PrecludesV: []ids.ID{id2},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id3},
					expectedAccepted:      []ids.ID{id3},
					expectedRejected:      []ids.ID{id2},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
				id3: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx2 precludes tx3; tx2 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id2},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      []ids.ID{id1, id3},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
				id3: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx2/tx3 mutually exclusive; tx1 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id2},
					PrecludesV:   []ids.ID{id2},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
					expectedRejected:      []ids.ID{id2},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
				id3: nil,
			},
		}, {
			name: "tx1/tx2 mutually exclusive; no deps; tx2/tx3 mutually exclusive; tx2 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id2},
					PrecludesV:   []ids.ID{id2},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id2},
					expectedAccepted:      []ids.ID{id2},
					expectedRejected:      []ids.ID{id1, id3},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
				id3: nil,
			},
		}, {
			name: "tx1/tx2/tx3 all mutually exclusive; no deps; tx1 accepted",
			txs: []*TestTx{
				{
					TestDecidable: choices.TestDecidable{
						IDV:     id1,
						StatusV: choices.Processing,
					},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id2,
						StatusV: choices.Processing,
					},
					PrecludesV:   []ids.ID{id1},
					PrecludedByV: []ids.ID{id1},
				}, {
					TestDecidable: choices.TestDecidable{
						IDV:     id3,
						StatusV: choices.Processing,
					},
					PrecludedByV: []ids.ID{id1, id2},
					PrecludesV:   []ids.ID{id1, id2},
				},
			},
			accepts: []accept{
				{
					toConditionallyAccept: []ids.ID{id1},
					expectedAccepted:      []ids.ID{id1},
					expectedRejected:      []ids.ID{id2, id3},
				},
			},
			expectedPrecludedBy: map[ids.ID][]ids.ID{
				id1: nil,
				id2: nil,
				id3: nil,
			},
		},
	}

	for _, test := range tests {
		t.Run(
			test.name,
			func(t *testing.T) {
				c := New()
				txIDToTxs := map[ids.ID]*TestTx{}

				// Map each txID to its tx
				for _, tx := range test.txs {
					txIDToTxs[tx.ID()] = tx

				}

				// Set each tx's dependencies
				for _, tx := range test.txs {
					for _, depID := range tx.dependenciesIDsV {
						depTx, ok := txIDToTxs[depID]
						if !ok {
							t.Fatalf("there is no tx %s", depID)
						}
						tx.DependenciesV = append(tx.DependenciesV, depTx)
					}
				}

				// Add the txs
				for _, tx := range test.txs {
					assert.NoError(t, c.Add(tx))
				}

				// Conditionally accept each tx in [accepts]
				for _, accept := range test.accepts {
					for _, toCondAccept := range accept.toConditionallyAccept {
						c.Accept(toCondAccept)
					}

					// Make sure the correct txs are reported as acceptable/rejectable
					acceptable, rejectable := c.Updateable()
					assert.Len(t, acceptable, len(accept.expectedAccepted))
					assert.Len(t, rejectable, len(accept.expectedRejected))

					// Accept each acceptable tx
					for _, acceptableTx := range acceptable {
						assert.Contains(t, accept.expectedAccepted, acceptableTx.ID())
						assert.NoError(t, acceptableTx.Accept())
					}
					// Reject each rejectable tx
					for _, rejectableTx := range rejectable {
						assert.Contains(t, accept.expectedRejected, rejectableTx.ID())
						assert.NoError(t, rejectableTx.Reject())
					}
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
			})
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
