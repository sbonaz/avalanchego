package snowstorm

import (
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/snow/choices"
)

func TestTopologicalSort(t *testing.T) {
	tx0 := &TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
	}

	tx1 := &TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
	}
	tx1.DependenciesV = append(tx1.DependenciesV, tx0)

	// Case: One element
	sorted, err := TopologicalSort([]Tx{tx1})
	if err != nil {
		t.Fatal(err)
	} else if sorted[0].ID() != tx1.ID() {
		t.Fatal("first element should be tx1")
	}

	// tx0 --> tx1 (tx1 depends on tx0)

	tx2 := &TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
	}
	tx2.DependenciesV = append(tx2.DependenciesV, tx1)

	// tx0 --> tx1 --> tx2

	tx3 := &TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
	}
	tx3.DependenciesV = append(tx3.DependenciesV, tx2)

	tx4 := &TestTx{
		TestDecidable: choices.TestDecidable{
			IDV:     ids.GenerateTestID(),
			StatusV: choices.Processing,
		},
	}
	tx4.DependenciesV = append(tx4.DependenciesV, tx2)

	// tx0 --> tx1 --> tx2 --> tx3
	//                  |
	//                  v
	//                 tx4

	// Case 2: Simple split
	sorted, err = TopologicalSort([]Tx{tx1, tx3, tx2, tx4})
	if err != nil {
		t.Fatal(err)
	} else if len(sorted) != 4 {
		t.Fatal("should have length 4")
	} else if sorted[0].ID() != tx1.ID() {
		t.Fatal("first element should be tx1")
	} else if sorted[1].ID() != tx2.ID() {
		t.Fatal("second element should be tx2")
	}

	// Case 3: Multiple dependencies
	tx3.DependenciesV = append(tx3.DependenciesV, tx4)

	// tx0 --> tx1 --> tx2 --> tx3
	//				     |     ^
	//                   |    /
	//                   v   /
	//                  tx4 /
	sorted, err = TopologicalSort([]Tx{tx1, tx3, tx2, tx4})
	if err != nil {
		t.Fatal(err)
	} else if len(sorted) != 4 {
		t.Fatal("should have length 4")
	} else if sorted[0].ID() != tx1.ID() {
		t.Fatal("first element should be tx1")
	} else if sorted[1].ID() != tx2.ID() {
		t.Fatal("second element should be tx2")
	} else if sorted[2].ID() != tx4.ID() {
		t.Fatal("second element should be tx4")
	} else if sorted[3].ID() != tx3.ID() {
		t.Fatal("second element should be tx3")
	}

	// Case 5: Cycle
	tx1.DependenciesV = append(tx1.DependenciesV, tx3)

	//          ________________
	//          |               |
	//          v               |
	// tx0 --> tx1 --> tx2 --> tx3
	//				     |     ^
	//                   |    /
	//                   v   /
	//                  tx4 /
	sorted, err = TopologicalSort([]Tx{tx1, tx3, tx2, tx4})
	if err == nil {
		t.Fatal("should have errored due to cycle")
	}
}
