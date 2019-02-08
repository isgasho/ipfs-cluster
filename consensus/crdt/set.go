package crdt

import (
	"context"
	"fmt"
	"io"

	"github.com/ipfs/ipfs-cluster/api"
	pb "github.com/ipfs/ipfs-cluster/consensus/crdt/pb"
	"github.com/ipfs/ipfs-cluster/state"
	"go.opencensus.io/trace"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

var (
	elemsNs = "elems"
	tombsNs = "tombs"
)

var ErrStateModify = "the CRDT state cannot be modified directly"

// crdtSet manages the Cluster State (a KV-store) using a AW-OR-Set Merkle-CRDT.
// It chooses the highest known Value for a Key in the Merkle-Clock as the
// current Value. When two values have the same height, it chooses by
// alphabetically sorting their Merkle-CRDT block CIDs.
type crdtSet struct {
	namespace ds.Key
	store     ds.Datastore
	cState    state.State
}

func newCRDTSet(
	d ds.Datastore,
	namespace ds.Key,
	st state.State,
) (*crdtSet, error) {

	dset := &crdtSet{
		namespace: namespace,
		store:     d,
		cState:    st,
	}
	return dset
}

// returns the new delta-set for adding
func (dset *crdtSet) add(e ...*pb.Element) (*pb.Delta, error) {
	return &pb.Delta{
		Elements:   e,
		Tombstones: nil,
	}, nil
}

// returns the new delta-set for removing
func (dset *crdtSet) rmv(cids ...cid.Cid) (*pb.Delta, error) {
	delta := &pb.Delta{
		Elements:   nil,
		Tombstones: make([]*pb.Element, 0),
	}

	for _, c := range cids {
		// /namespace/elems/<cid>
		prefix := dset.namespace.ChildString(elemsNs).Child(dshelp.CidToDsKey(c))

		q := query.Query{
			Prefix:   prefix.String(),
			KeysOnly: true,
		}

		results, err := dset.store.Query(q)
		if err != nil {
			return nil, err
		}
		defer results.Close()

		for r := range results.Next() {
			block, err := cidFromResult(r)
			if err != nil {
				return delta, err
			}

			delta.Tombstones = append(delta.tombstones, &pb.Element{
				Cid:   c.String(),
				Block: block.String(),
			})
		}
	}
	return delta
}

// put stores elements in the tombstones or the elements set.
func (dset *crdtSet) put(elems []*pb.Element, block cid.Cid, setNs string) error {
	store := dset.store
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store = batchingDs.Batch()
	}

	blockStr := block.String()

	for _, e := range elems {
		if block.Defined() {
			e.Block = blockStr // overwrite the block
		}
		// we will add to /namespace/<setNs>/<cid>/<block>
		suffix, err := pbElemDsKey(e)
		if err != nil {
			return err
		}
		k := dset.namespace.ChildString(setNs).Child(suffix)
		err := dset.store.Put(key, nil)
		if err != nil {
			return err
		}
	}

	if batching {
		err := store.(ds.Batch).Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (dset *crdtSet) merge(d *pb.Delta, block cid.Cid) error {
	ctx := context.TODO()

	// store the set
	err := dset.put(d.GetElements(), block, elemsNs)
	if err != nil {
		return err
	}

	// update Cluster state for new elements with
	// higher height than current ones.
	for _, e := range d.GetElements() {
		addCid, err := cid.Decode(e.Cid)
		if err != nil {
			return err
		}
		curPin, ok := dset.cState.Get(ctx, addCid)
		// put if our height is higher
		if !ok || curPin.Height < d.GetHeight() {
			e.Block = block.String()
			dset.cState.Add(ctx, pinFromElement(e, d.GetHeight()))
		}
	}

	// store the tombstones
	return dset.put(d.Tombstones, cid.Undef, tombsNs)
}

// inSet returns if we have a Cid. For it we must have at least one element
// for that CID which is not tombstoned.
func (dset *crdtSet) inSet(c cid.Cid) (bool, error) {
	// /namespace/elems/<cid>
	prefix := dset.namespace.ChildString(elemsNs).Child(dshelp.CidToDsKey(c))
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := dset.store.Query(q)
	if err != nil {
		return false, err
	}
	defer results.Close()

	// range for all the <cid> in the elems set.
	for r := range results.Next() {
		block, err := cidFromResult(r)
		if err != nil {
			return false, err
		}

		// if not tombstoned, we have it
		if !dset.inTombsCidBlock(c, block) {
			return true, nil
		}
	}
	return false, nil
}

func (dset *crdtSet) inElemsCidBlock(c, block cid.Cid) (bool, error) {
	blockKey := dshelp.CidToDsKey(c).Child(dshelp.CidToDsKey(block))
	elemKey := dset.namespace.ChildString(elemsNs).Child(blockKey)
	return dset.Store.Has(elemKey)
}

func (dset *crdtSet) inTombsCidBlock(c, block cid.Cid) (bool, error) {
	blockKey := dshelp.CidToDsKey(c).Child(dshelp.CidToDsKey(block))
	tombKey := dset.namespace.ChildString(tombsNs).Child(blockKey)
	return dset.Store.Has(tombKey)
}

// inSet returns if the given cid/block is in elems and not in tombs (and
// thus, it is an elemement of the set).
func (dset *crdtSet) inSetCidBlock(c, block cid.Cid) (bool, error) {
	inTombs, err := dset.inTombsCidBlock(c, block)
	if err != nil {
		return false, err
	}
	if inTombs {
		return false, nil
	}

	return dset.inElemsCidBlock(c, block)
}

// ####################################################################
// Implementation of the State interface
// ####################################################################

// Add adds a Pin to the internal map.
func (dset *crdtSet) Add(ctx context.Context, c api.Pin) error {
	return ErrStateModify
}

// Rm removes a Cid from the internal map.
func (dset *crdtSet) Rm(ctx context.Context, c cid.Cid) error {
	return ErrStateModify
}

// Get returns Pin information for a CID.
func (dset *crdtSet) Get(ctx context.Context, c cid.Cid) (api.Pin, bool) {
	// We can only GET an element if it's part of the CRDT Set.

	// An element not being in cState implies it's not in the CRDT set
	// either because it was never added or because it was tombstoned and
	// we removed it last time it was Get.

	// An element being in cState implies it is at least in the elems set.

	ctx, span := trace.StartSpan(ctx, fmt.Sprintf("%s/Get", dset.namespace))
	defer span.End()

	pin, exists := dset.cState.Get(ctx, c)
	if !exists { // tombstoned or never existed
		return api.PinCid(c), false
	}

	// We have an existing element. Check if tombstoned.

	// An alternative here is to store the block number in the pin
	// and check directly with inTombsCidBlock. However, better to
	// save all that space. Single queries should be fast enough (?).
	if !dset.inSet(c) {
		// remove so next time we do not have to do lookups
		// on our CRDT sets.
		dset.cState.Rm(ctx, c)
		return api.PinCid(c), false
	}

	return pin, true
}

// Has returns true if the Cid belongs to the State.
func (dset *crdtSet) Has(ctx context.Context, c cid.Cid) bool {
	ctx, span := trace.StartSpan(ctx, fmt.Sprintf("%s/Has", dset.namespace))
	defer span.End()

	// Reimplementing Get here using Has only marginally
	// faster. Let's opt for less code.
	_, ok := dset.Get(ctx, c)
	return ok
}

// List provides the list of tracked Pins.
func (dset *crdtSet) List(ctx context.Context) []api.Pin {
	ctx, span := trace.StartSpan(ctx, fmt.Sprintf("%s/List", dset.namespace))
	defer span.End()

	// this one will be horribly slow
	// need to turn this into a streaming call
	// which returns a channel.
	pins := dset.cState.List(ctx)
	var filtered api.Pin

	for _, p := range pins {
		if !dset.inSet(p.Cid) {
			// take the chance to clean up
			dset.cState.Rm(ctx, p.Cid)
			continue
		}
		filtered = append(filtered, p)
	}

	return filtered
}

// Migrate restores a snapshot from the state's internal bytes and if
// necessary migrates the format to the current version.
func (dset *crdtSet) Migrate(ctx context.Context, r io.Reader) error {
	// Not sure if it should be allowed to call Migrate at all.
	return dset.cState.Migrate(ctx, r)
}

// GetVersion returns the current version of this state object.
func (dset *crdtSet) GetVersion() int {
	return dset.cState.GetVersion()
}

// Marshal encodes the state to the given writer. This implements
// go-libp2p-raft.Marshable.
func (dset *crdtSet) Marshal(w io.Writer) error {
	// Not sure if it should be allowed.
	return dset.cState.Marshal(w)
}

// Unmarshal decodes the state from the given reader. This implements
// go-libp2p-raft.Marshable.
func (dset *crdtSet) Unmarshal(r io.Reader) error {
	// Not sure if it should be allowed.
	return dset.cState.Unmarshal(r)
}
