package crdt

import (
	"context"
	"sync"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/ipfs-cluster/state"
	"github.com/pkg/errors"
)

// datastore prefixes
const (
	blocksNs = "blocks"
	headsNs  = "heads"
	setNs    = "set"
)

var (
	messageBufferLen = 10
)

// merkleCRDT implements a Merkle-CRDT KV-Store.
type merkleCRDT struct {
	// a channel to receive broadcasted blocks
	sub <-chan cid.Cid
	pub chan<- cid.Cid

	// A way of fetching and putting blocks to IPFS
	dags crdtDAGService

	// permanent storage
	namespace ds.Key
	store     ds.Datastore
	set       *crdtSet
	heads     *heads
	blocks    *blocks

	// Actual Cluster state (that we manage)
	cState state.State
}

func newCRDT(
	sub <-chan cid.Cid,
	pub <-chan cid.Cid,
	dags *crdtDAGService,
	store ds.Datastore,
	namespace ds.Key,
	st state.State,
) (*merkleCRDT, error) {

	// <namespace>/<topic>/set
	fullSetNs := namespace.ChildString(setNs)
	// <namespace>/<topic>/heads
	fullHeadsNs := namespace.ChildString(headsNs)
	// <namespace>/<topic>/blocks
	fullBlocksNs := namespace.ChildString(blocksNs)

	set := newCRDTSet(store, fullSetNs, st)
	heads := newHeads(store, fullHeadsNs)
	blocks := newBlocks(store, fullBlocksNs)

	mcrdt := &merkleCRDT{
		sub:       sub,
		pub:       pub,
		ipfs:      ipfs,
		store:     store,
		namespace: ds.Key,
		set:       set,
		heads:     heads,
		blocks:    blocks,
		cState:    st,
	}

	go mcrdt.subHandle()

	return mcrdt
}

// goroutine
func (mcrdt *merkleCRDT) subHandle() {
	for c := range sub {
		mcrdt.handleBlock(c)
	}
}

// handleBlock takes care of applying vetting, retrieving and applying
// CRDT blocks to the MerkleCRDT.
func (mcrdt *merkleCRDT) handleBlock(c cid.Cid) {

	// Ignore already known blocks.
	// This includes the case when the block is a current
	// head.
	known, err := mcrdt.blocks.IsKnown(c)
	if err != nil {
		logger.Errorf("error checking for known block: %s", err)
		return
	}
	if known {
		return
	}

	// It is the first time we see this block. Process it.
	// Lock and load heads.
	err := mcrdt.heads.LockHeads()
	if err != nil {
		logger.Errorf("error locking heads: %s", err)
		return
	}
	// At the end, write any new heads, remove old ones and unlock.
	defer mcrdt.heads.UnlockHeads()

	// Walk down from this block.
	err := mcrdt.walkBranch(c, c)
	if err != nil {
		logger.Errorf("error walking new branch: %s", err)
		return
	}
}

// walkBranch walks down a branch and applies its deltas
// until it arrives to a known head or the bottom.
// The given CID is assumed to not be a known block.
// and will be fetched.
func (mcrdt *merkleCRDT) walkBranch(current, top cid.Cid) error {
	// TODO: Pre-fetching of children?
	nd, delta, err := mcrdt.blocks.Fetch(context.TODO(), c)
	if err != nil {
		return errors.Wrapf(err, "fetching block %s", c)
	}

	// merge the delta
	err := mcrdt.set.merge(delta, c)
	if err != nil {
		return errors.Wrapf(err, "merging delta from %s", c)
	}

	var wg sync.WaitGroup

	// walkToChildren
	for _, l := range nd.Links() {
		child := l.Cid
		if mcrdt.heads.IsHead(child) {
			// reached one of the current heads. Replace it with
			// the tip of this branch
			mcrdt.heads.Replace(child, top)
			continue
		}
		if mcrdt.blocks.IsKnown(child) {
			// we reached a non-head node in the known tree.
			// This means our top block is a new head.
			mcrdt.heads.Add(topBlock)
			continue
		}

		// TODO: parallize
		err := mcrdt.walkBranch(child, top)
		if err != nil {
			return err
		}
	}

	return nil
}
