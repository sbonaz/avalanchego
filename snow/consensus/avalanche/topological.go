// (c) 2019-2020, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package avalanche

import (
	"fmt"

	"github.com/ava-labs/gecko/ids"
	"github.com/ava-labs/gecko/snow"
	"github.com/ava-labs/gecko/snow/choices"
	"github.com/ava-labs/gecko/snow/consensus/snowstorm"
)

const (
	minMapSize = 16
)

// TopologicalFactory implements Factory by returning a topological struct
type TopologicalFactory struct{}

// New implements Factory
func (TopologicalFactory) New() Consensus { return &Topological{} }

// TODO: Implement pruning of decisions.
// To perfectly preserve the protocol, this implementation will need to store
// the hashes of all accepted decisions. It is possible to add a heuristic that
// removes sufficiently old decisions. However, that will need to be analyzed to
// ensure safety. It is doable when adding in a weak synchrony assumption.

// Topological performs the avalanche algorithm by utilizing a topological sort
// of the voting results. Assumes that vertices are inserted in topological
// order.
type Topological struct {
	metrics

	// Gets a vertex by its ID.
	// Returns error if the vertex was not found.
	VertexGetter

	// Context used for logging
	ctx *snow.Context

	// Threshold for confidence increases
	params Parameters

	// Maps vtxID -> vtx
	nodes map[[32]byte]Vertex

	// Tracks the conflict relations
	cg snowstorm.Consensus

	// preferred is the frontier of vtxIDs that are strongly preferred
	// virtuous is the frontier of vtxIDs that are strongly virtuous
	// orphans are the txIDs that are virtuous, but not preferred
	preferred, virtuous, orphans ids.Set

	// frontier is the set of vts that have no descendents
	frontier map[[32]byte]Vertex

	// preferenceCache is the cache for strongly preferred checks
	// virtuousCache is the cache for strongly virtuous checks
	preferenceCache, virtuousCache map[[32]byte]bool
}

type kahnNode struct {
	inDegree int
	votes    ids.BitSet
}

// Initialize implements the Avalanche interface
func (ta *Topological) Initialize(
	ctx *snow.Context,
	params Parameters,
	frontier []Vertex,
	vertexGetter VertexGetter,
	txManager snowstorm.TxManager,
) error {
	if err := params.Valid(); err != nil {
		return err
	}

	ta.ctx = ctx
	ta.params = params
	ta.VertexGetter = vertexGetter

	if err := ta.metrics.Initialize(ctx.Log, params.Namespace, params.Metrics); err != nil {
		return err
	}

	ta.nodes = make(map[[32]byte]Vertex, minMapSize)

	ta.cg = &snowstorm.Directed{}
	if err := ta.cg.Initialize(ctx, params.Parameters, txManager); err != nil {
		return err
	}

	ta.frontier = make(map[[32]byte]Vertex, minMapSize)
	for _, vtx := range frontier {
		ta.frontier[vtx.ID().Key()] = vtx
	}

	_, _, err := ta.updateFrontiers() // TODO: Make sure this is ok
	return err
}

// Parameters implements the Avalanche interface
func (ta *Topological) Parameters() Parameters { return ta.params }

// IsVirtuous implements the Avalanche interface
func (ta *Topological) IsVirtuous(tx snowstorm.Tx) bool { return ta.cg.IsVirtuous(tx) }

// Add implements the Avalanche interface.
// It adds a vertex to consensus.
// Returns:
//   1) The IDs of vertices accepted as a result of this operation.
//      Nil if there are none.
//   2) The IDs of vertices rejected as a result of this operation.
//      Nil if there are none.
func (ta *Topological) Add(vtx Vertex) (ids.Set, ids.Set, error) {
	ta.ctx.Log.AssertTrue(vtx != nil, "Attempting to insert nil vertex")

	vtxID := vtx.ID()
	key := vtxID.Key()
	if vtx.Status().Decided() {
		return nil, nil, nil // Already decided this vertex
	} else if _, exists := ta.nodes[key]; exists {
		return nil, nil, nil // Already inserted this vertex
	}

	ta.ctx.ConsensusDispatcher.Issue(ta.ctx.ChainID, vtxID, vtx.Bytes())

	txs, err := vtx.Txs()
	if err != nil {
		return nil, nil, err
	}
	for _, tx := range txs {
		if !tx.Status().Decided() {
			// Add the consumers to the conflict graph.
			if err := ta.cg.Add(tx); err != nil {
				return nil, nil, err
			}
		}
	}

	ta.nodes[key] = vtx // Add this vertex to the set of nodes
	ta.metrics.Issued(vtxID)

	return ta.update(vtxID) // Update the vertex and it's ancestry
}

// VertexIssued implements the Avalanche interface
func (ta *Topological) VertexIssued(vtx Vertex) bool {
	if vtx.Status().Decided() {
		return true
	}
	_, ok := ta.nodes[vtx.ID().Key()]
	return ok
}

// TxIssued implements the Avalanche interface
func (ta *Topological) TxIssued(tx snowstorm.Tx) bool { return ta.cg.Issued(tx) }

// Orphans implements the Avalanche interface
func (ta *Topological) Orphans() ids.Set { return ta.orphans }

// Virtuous implements the Avalanche interface
func (ta *Topological) Virtuous() ids.Set { return ta.virtuous }

// Preferences implements the Avalanche interface
func (ta *Topological) Preferences() ids.Set { return ta.preferred }

// RecordPoll implements the Avalanche interface
// Returns:
//   1) The IDs of vertices accepted as a result of this operation.
//      Nil if there are none.
//   2) The IDs of vertices rejected as a result of this operation.
//      Nil if there are none.
func (ta *Topological) RecordPoll(responses ids.UniqueBag) (ids.Set, ids.Set, error) {
	// If it isn't possible to have alpha votes for any transaction, then we can
	// just reset the confidence values in the conflict graph and not perform
	// any traversals.
	partialVotes := ids.BitSet(0)
	for _, vote := range responses.List() {
		votes := responses.GetSet(vote)
		partialVotes.Union(votes)
		if partialVotes.Len() >= ta.params.Alpha {
			break
		}
	}
	if partialVotes.Len() < ta.params.Alpha {
		// Skip the traversals.
		_, err := ta.cg.RecordPoll(ids.Bag{})
		return nil, nil, err
	}

	// Set up the topological sort: O(|Live Set|)
	kahns, leaves, err := ta.calculateInDegree(responses)
	if err != nil {
		return nil, nil, err
	}
	// Collect the votes for each transaction: O(|Live Set|)
	votes, err := ta.pushVotes(kahns, leaves)
	if err != nil {
		return nil, nil, err
	}
	// Update the conflict graph: O(|Transactions|)
	ta.ctx.Log.Verbo("Updating consumer confidences based on:\n%s", &votes)
	if updated, err := ta.cg.RecordPoll(votes); !updated || err != nil {
		// If the transaction statuses weren't changed, there is no need to
		// perform a traversal.
		return nil, nil, err
	}
	// Update the dag: O(|Live Set|)
	return ta.updateFrontiers()
}

// Quiesce implements the Avalanche interface
func (ta *Topological) Quiesce() bool { return ta.cg.Quiesce() }

// Finalized implements the Avalanche interface
func (ta *Topological) Finalized() bool { return ta.cg.Finalized() }

// Takes in a list of votes and sets up the topological ordering. Returns the
// reachable section of the graph annotated with the number of inbound edges and
// the non-transitively applied votes. Also returns the list of leaf nodes.
func (ta *Topological) calculateInDegree(responses ids.UniqueBag) (
	map[[32]byte]kahnNode,
	[]ids.ID,
	error,
) {
	kahns := make(map[[32]byte]kahnNode, minMapSize)
	leaves := ids.Set{}

	for _, vote := range responses.List() {
		key := vote.Key()
		// If it is not found, then the vote is either for something decided,
		// or something we haven't heard of yet.
		if vtx := ta.nodes[key]; vtx != nil {
			kahn, previouslySeen := kahns[key]
			// Add this new vote to the current bag of votes
			kahn.votes.Union(responses.GetSet(vote))
			kahns[key] = kahn

			if !previouslySeen {
				// If I've never seen this node before, it is currently a leaf.
				leaves.Add(vote)
				parents, err := vtx.Parents()
				if err != nil {
					return nil, nil, err
				}
				kahns, leaves, err = ta.markAncestorInDegrees(kahns, leaves, parents)
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}

	return kahns, leaves.List(), nil
}

// adds a new in-degree reference for all nodes
func (ta *Topological) markAncestorInDegrees(
	kahns map[[32]byte]kahnNode,
	leaves ids.Set,
	parentIDs []ids.ID,
) (map[[32]byte]kahnNode, ids.Set, error) {
	frontier := make([]Vertex, 0, len(parentIDs))
	for _, parentID := range parentIDs {
		parent, err := ta.GetVertex(parentID)
		if err != nil {
			return nil, nil, fmt.Errorf("couldn't get vertex %s", parentID)
		}
		// The vertex may have been decided, no need to vote in that case
		if !parent.Status().Decided() {
			frontier = append(frontier, parent)
		}
	}

	for len(frontier) > 0 {
		newLen := len(frontier) - 1
		current := frontier[newLen]
		frontier = frontier[:newLen]

		currentID := current.ID()
		currentKey := currentID.Key()
		kahn, alreadySeen := kahns[currentKey]
		// I got here through a transitive edge, so increase the in-degree
		kahn.inDegree++
		kahns[currentKey] = kahn

		if kahn.inDegree == 1 {
			// If I am transitively seeing this node for the first
			// time, it is no longer a leaf.
			leaves.Remove(currentID)
		}

		if !alreadySeen {
			// If I am seeing this node for the first time, I need to check its
			// parents
			parentIDs, err := current.Parents()
			if err != nil {
				return nil, nil, err
			}
			for _, parentID := range parentIDs {
				parent, err := ta.GetVertex(parentID)
				if err != nil {
					return nil, nil, fmt.Errorf("couldn't get vertex %s", parentID)
				}
				// No need to traverse to a decided vertex
				if !parent.Status().Decided() {
					frontier = append(frontier, parent)
				}
			}
		}
	}
	return kahns, leaves, nil
}

// count the number of votes for each operation
func (ta *Topological) pushVotes(
	kahnNodes map[[32]byte]kahnNode,
	leaves []ids.ID,
) (ids.Bag, error) {
	votes := make(ids.UniqueBag)
	txConflicts := make(map[[32]byte]ids.Set, minMapSize)

	for len(leaves) > 0 {
		newLeavesSize := len(leaves) - 1
		leaf := leaves[newLeavesSize]
		leaves = leaves[:newLeavesSize]

		key := leaf.Key()
		kahn := kahnNodes[key]

		if vtx := ta.nodes[key]; vtx != nil {
			txs, err := vtx.Txs()
			if err != nil {
				return ids.Bag{}, err
			}
			for _, tx := range txs {
				// Give the votes to the consumer
				txID := tx.ID()
				votes.UnionSet(txID, kahn.votes)

				// Map txID to set of Conflicts
				txKey := txID.Key()
				if _, exists := txConflicts[txKey]; !exists {
					txConflicts[txKey] = ta.cg.Conflicts(tx)
				}
			}

			parentIDs, err := vtx.Parents()
			if err != nil {
				return ids.Bag{}, err
			}
			for _, parentID := range parentIDs {
				parentIDKey := parentID.Key()
				if depNode, notPruned := kahnNodes[parentIDKey]; notPruned {
					depNode.inDegree--
					// Give the votes to my parents
					depNode.votes.Union(kahn.votes)
					kahnNodes[parentIDKey] = depNode

					if depNode.inDegree == 0 {
						// Only traverse into the leaves
						leaves = append(leaves, parentID)
					}
				}
			}
		}
	}

	// Create bag of votes for conflicting transactions
	conflictingVotes := make(ids.UniqueBag)
	for txHash, conflicts := range txConflicts {
		txID := ids.NewID(txHash)
		for conflictTxHash := range conflicts {
			conflictTxID := ids.NewID(conflictTxHash)
			conflictingVotes.UnionSet(txID, votes.GetSet(conflictTxID))
		}
	}

	votes.Difference(&conflictingVotes)
	return votes.Bag(ta.params.Alpha), nil
}

// If I've already checked, do nothing
// If I'm decided, cache the preference and return
// At this point, I must be live
// I now try to accept all my consumers
// I now update all my ancestors
// If any of my parents are rejected, reject myself
// If I'm preferred, remove all my ancestors from the preferred frontier, add
//     myself to the preferred frontier
// If all my parents are accepted and I'm acceptable, accept myself
// Returns:
//   1) The IDs of vertices accepted as a result of this operation.
//      Nil if there are none.
//   2) The IDs of vertices rejected as a result of this operation.
//      Nil if there are none.
func (ta *Topological) update(vtxID ids.ID) (ids.Set, ids.Set, error) {
	vtxKey := vtxID.Key()
	vtx, err := ta.GetVertex(vtxID)
	if err != nil {
		return nil, nil, fmt.Errorf("couldn't update vertex %s: not found", vtxID)
	}

	if _, cached := ta.preferenceCache[vtxKey]; cached {
		return nil, nil, nil // This vertex has already been updated
	}

	switch vtx.Status() {
	case choices.Accepted:
		ta.preferred.Add(vtxID)   // I'm preferred
		ta.virtuous.Add(vtxID)    // Accepted is defined as virtuous
		ta.frontier[vtxKey] = vtx // I have no descendents yet
		ta.preferenceCache[vtxKey] = true
		ta.virtuousCache[vtxKey] = true
		return nil, nil, nil
	case choices.Rejected:
		// I'm rejected
		ta.preferenceCache[vtxKey] = false
		ta.virtuousCache[vtxKey] = false
		return nil, nil, nil
	}

	acceptable := true  // If the batch is accepted, this vertex is acceptable
	rejectable := false // If I'm rejectable, I must be rejected
	preferred := true
	virtuous := true
	txs, err := vtx.Txs()
	if err != nil {
		return nil, nil, err
	}
	preferences := ta.cg.Preferences()
	virtuousTxs := ta.cg.Virtuous()

	for _, tx := range txs {
		txID := tx.ID()
		s := tx.Status()
		if s == choices.Rejected {
			// If I contain a rejected consumer, I am rejectable
			rejectable = true
			preferred = false
			virtuous = false
		}
		if s != choices.Accepted {
			// If I contain a non-accepted consumer, I am not acceptable
			acceptable = false
			preferred = preferred && preferences.Contains(txID)
			virtuous = virtuous && virtuousTxs.Contains(txID)
		}
	}

	parentIDs, err := vtx.Parents()
	if err != nil {
		return nil, nil, err
	}
	accepted := ids.Set{}
	rejected := ids.Set{}

	// Update all of my dependencies
	for _, parentID := range parentIDs {
		acc, rej, err := ta.update(parentID)
		if err != nil {
			return nil, nil, err
		}
		accepted.Union(acc)
		rejected.Union(rej)

		key := parentID.Key()
		preferred = preferred && ta.preferenceCache[key]
		virtuous = virtuous && ta.virtuousCache[key]
	}

	// Check my parent statuses
	for _, parentID := range parentIDs {
		parent, err := ta.GetVertex(parentID)
		if err != nil {
			return nil, nil, fmt.Errorf("couldn't get vertex %s", parentID)
		}
		if status := parent.Status(); status == choices.Rejected {
			// My parent is rejected, so I should be rejected
			if err := vtx.Reject(); err != nil {
				return nil, nil, err
			}
			rejected.Add(vtxID)
			ta.ctx.ConsensusDispatcher.Reject(ta.ctx.ChainID, vtxID, vtx.Bytes())
			delete(ta.nodes, vtxKey)
			ta.metrics.Rejected(vtxID)

			ta.preferenceCache[vtxKey] = false
			ta.virtuousCache[vtxKey] = false
			return accepted, rejected, nil
		} else if status != choices.Accepted {
			acceptable = false // My parent isn't accepted, so I can't be
		}
	}

	// Technically, we could also check to see if there are direct conflicts
	// between this vertex and a vertex in it's ancestry. If there does exist
	// such a conflict, this vertex could also be rejected. However, this would
	// require a traversal. Therefore, this memory optimization is ignored.
	// Also, this will only happen from a byzantine node issuing the vertex.
	// Therefore, this is very unlikely to actually be triggered in practice.

	// Remove all my parents from the frontier
	for _, parentID := range parentIDs {
		delete(ta.frontier, parentID.Key())
	}
	ta.frontier[vtxKey] = vtx // I have no descendents yet

	ta.preferenceCache[vtxKey] = preferred
	ta.virtuousCache[vtxKey] = virtuous

	if preferred {
		ta.preferred.Add(vtxID) // I'm preferred
		for _, parentID := range parentIDs {
			ta.preferred.Remove(parentID) // My parents aren't part of the frontier
		}

		for _, tx := range txs {
			if tx.Status() != choices.Accepted {
				ta.orphans.Remove(tx.ID())
			}
		}
	}

	if virtuous {
		ta.virtuous.Add(vtxID) // I'm virtuous
		for _, parentID := range parentIDs {
			ta.virtuous.Remove(parentID) // My parents aren't part of the frontier
		}
	}

	switch {
	case acceptable:
		// I'm acceptable, why not accept?
		err := vtx.Accept()
		if err != nil {
			return nil, nil, err
		}
		accepted.Add(vtxID)
		ta.ctx.ConsensusDispatcher.Accept(ta.ctx.ChainID, vtxID, vtx.Bytes())
		delete(ta.nodes, vtxKey)
		ta.metrics.Accepted(vtxID)
	case rejectable:
		// I'm rejectable, why not reject?
		err := vtx.Reject()
		if err != nil {
			return nil, nil, err
		}
		rejected.Add(vtxID)
		ta.ctx.ConsensusDispatcher.Reject(ta.ctx.ChainID, vtxID, vtx.Bytes())
		delete(ta.nodes, vtxKey)
		ta.metrics.Rejected(vtxID)
	}
	return accepted, rejected, nil
}

// Update the frontier sets
// Returns:
//   1) The IDs of vertices accepted as a result of this operation.
//      Nil if there are none.
//   2) The IDs of vertices rejected as a result of this operation.
//      Nil if there are none.
func (ta *Topological) updateFrontiers() (ids.Set, ids.Set, error) {
	vts := ta.frontier

	ta.preferred.Clear()
	ta.virtuous.Clear()
	ta.orphans.Clear()
	ta.frontier = make(map[[32]byte]Vertex, minMapSize)
	ta.preferenceCache = make(map[[32]byte]bool, minMapSize)
	ta.virtuousCache = make(map[[32]byte]bool, minMapSize)

	ta.orphans.Union(ta.cg.Virtuous()) // Initially, nothing is preferred

	accepted := ids.Set{}
	rejected := ids.Set{}
	for _, vtx := range vts {
		// Update all the vertices that were in my previous frontier
		acc, rej, err := ta.update(vtx.ID())
		if err != nil {
			return nil, nil, err
		}
		accepted.Union(acc)
		rejected.Union(rej)
	}
	return accepted, rejected, nil
}
