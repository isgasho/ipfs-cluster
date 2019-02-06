package crdt

import (
	pb "github.com/ipfs/ipfs-cluster/consensus/crdt/pb"
	"github.com/ipfs/ipfs-cluster/state"

	cid "github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

var (
	elemsNs = "elems"
	tombsNs = "tombs"
)

// crdtSet manages the Cluster State (a KV-store) using a AW-OR-Set Merkle-CRDT.
// It chooses the highest known Value for a Key in the Merkle-Clock as the
// current Value. When two values have the same height, it chooses by
// alphabetically sorting their Merkle-CRDT block CIDs.
type crdtSet struct {
	namespace ds.Key
	store     ds.Datastore
	cState    state.State

	dags *crdtDAGService
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
		prefix := dset.sKey.Child(dshelp.CidToDsKey(c))

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

func (dset *crdtSet) put(elems []*pb.Element, block cid.Cid, prefix ds.Key) error {
	store := dset.store
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store = batchingDs.Batch()
	}

	for _, e := range elems {
		if block.Defined() {
			e.Block = block // set the block
		}
		// we will add to /prefix/<cid>/<block>
		suffix, err := pbElemDsKey(e)
		if err != nil {
			return err
		}
		key := prefix.Child(suffix)
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
	// store the set
	err := dset.put(d.Elements, block, dset.sKey)
	if err != nil {
		return err
	}
	// store the tombstones
	return dset.put(d.Tombstones, cid.Undef, dset.tKey)
}

// has returns if we have a Cid. For it we must have at
// least one element for that CID which is not tombstoned.
func (dset *crdtSet) has(c cid.Cid) (bool, error) {
	prefix := dset.sKey.Child(dshelp.CidToDsKey(c))
	q := query.Query{
		Prefix:   prefix.String(),
		KeysOnly: true,
	}

	results, err := dset.store.Query(q)
	if err != nil {
		return false, err
	}
	defer results.Close()

	// range for all the <cid> in the s set.
	for r := range results.Next() {
		block, err := cidFromResult(r)
		if err != nil {
			return false, err
		}

		// if not tombstoned, we have it
		if !dset.hasElement(&pb.Element{Cid: c, Block: block}) {
			return true, nil
		}
	}
	return false, nil
}

// hasElement returns if we have an element (it must be
// in the s set but not in the tombstone set).
func (dset *crdtSet) hasElement(e *pb.Element) (bool, error) {
	dsKey, err := pbElemDsKey(e)
	if err != nil {
		return false, err
	}
	inElems, err := dset.Store.Has(dset.sKey.Child(dsKey))
	if err != nil || !inElems {
		return false, err
	}

	inTombs, err := dset.Store.Hash(dset.tKey.Child(dsKey))
	return !inTombs, err
}
