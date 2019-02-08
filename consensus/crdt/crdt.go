package crdt

import (
	"context"
	"sync"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/state"
	"github.com/pkg/errors"
)

// datastore prefixes
const (
	blocksNs = "blocks"
	headsNs  = "heads"
	setNs    = "set"
)

// merkleCRDT implements a Merkle-CRDT KV-Store.
type merkleCRDT struct {
	// Actual Cluster state (that we manage)
	cState state.State

	// permanent storage
	store     ds.Datastore
	namespace ds.Key
	set       *crdtSet
	heads     *heads
	blocks    *blocks

	dags      *crdtDAGService
	broadcast func(cid.Cid) error

	curDeltaMux sync.Mutex
	curDelta    *pb.Delta
}

func newCRDT(
	st state.State,
	store ds.Datastore,
	namespace ds.Key,

	dags *crdtDAGService,
	broadcast func(cid.Cid) error,
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
		cState:    st,
		store:     store,
		namespace: ds.Key,
		set:       set,
		heads:     heads,
		blocks:    blocks,
		dags:      dags,
		broadcast: broadcast,
	}

	return mcrdt
}

// HandleBlock takes care of applying vetting, retrieving and applying
// CRDT blocks to the MerkleCRDT.
func (mcrdt *merkleCRDT) HandleBlock(ctx context.Context, c cid.Cid) error {

	// Ignore already known blocks.
	// This includes the case when the block is a current
	// head.
	known, err := mcrdt.blocks.IsKnown(c)
	if err != nil {
		logger.Errorf("error checking for known block: %s", err)
		return nil
	}
	if known {
		return nil
	}

	// It is the first time we see this block. Process it.
	// Lock and load heads.
	err := mcrdt.heads.LockHeads()
	if err != nil {
		return errors.Wrap(err, "locking heads")
	}
	// At the end, write any new heads, remove old ones and unlock.
	defer mcrdt.heads.UnlockHeads()

	if mcrdt.heads.Len() == 0 { // no heads? We must be syncing from scratch
		err := mcrdt.dags.FetchRefs(ctx, -1)
		if err != nil {
			logger.Warning(errors.Wrap(err, "fetching refs"))
		}
	}

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
	nd, delta, err := mcrdt.dags.GetDelta(context.TODO(), c)
	if err != nil {
		return errors.Wrapf(err, "fetching block %s", c)
	}

	// merge the delta
	err := mcrdt.set.merge(delta, c)
	if err != nil {
		return errors.Wrapf(err, "merging delta from %s", c)
	}

	// var wg sync.WaitGroup

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

func (mcrdt *merkleCRDT) State() state.State {
	return mcrdt.set
}

func (mcrdt *merkleCRDT) AddToDelta(pin *api.Pin) {
	elem := pinToPbElem(pin)
	mcrdt.updateDelta(mcrdt.set.add(elem))
}

func (mcrdt *merkleCRDT) RmvToDelta(c cid.Cid) {
	mcrdt.updateDelta(mcrdt.set.rmv(c))
}

func (mcrdt *merkleCRDT) Add(pin *api.Pin) error {
	elem := pinToPbElem(pin)
	delta := mcrdt.set.add(elem)
	return mcrdt.publish(delta)
}

func (mcrdt *merkleCRDT) Rmv(c cid.Cid) error {
	delta := mcrdt.set.rmv(c)
	return mcrdt.publish(delta)
}

func (mcrdt *merkleCRDT) updateDelta(newDelta *pb.Delta) {
	mcrdt.curDeltaMux.Lock()
	defer mcrdt.curDeltaMux.Unlock()
	mcrdt.curDelta = deltaMerge(mcrdt.curDelta, newDelta)
}

// Create a block, write it to IPFS, publish the CID.
func (mcrdt *merkleCRDT) publish(deta *pb.Delta) {

}
