package crdt

import (
	cid "github.com/ipfs/go-cid"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

// blocks annotates processed blocks and provides utility
// to enable fast tree comparison.
type blocks struct {
	store  ds.Datastore
	prefix ds.Key
}

func newBlocks(store ds.Datastore, blocksPrefix ds.Key) {
	return &blocks{
		store:  store,
		prefix: blocksPrefix,
		ng:     ng,
	}
}

func (bb *blocks) key(c cid.Cid) ds.Key {
	return bb.prefix.Child(dshelp.CidToDsKey(c))
}

// IsKnown returns if a block has been added
func (bb *blocks) IsKnown(c cid.Cid) (bool, error) {
	return bb.store.Has(hh.key(c))
}

// Add marks a block as known
func (bb *blocks) Add(c cid.Cid) error {
	return bb.store.Put(hh.key(c), nil)
}
