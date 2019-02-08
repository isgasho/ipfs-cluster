package crdt

import (
	"sync"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore/query"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
)

// heads manages the current Merkle-CRDT heads.
// Processes should LockHeads() and UnlockHeads()
// to ensure the heads are not moved from under
// their feet.
type heads struct {
	glock sync.Mutex

	store ds.Datastore

	prefix ds.Key
	mux    sync.RWMutex
	heads  map[cid.Cid]cid.Cid
}

func newHeads(store ds.Datastore, headsPrefix ds.Key) {
	return &heads{
		store:  store,
		prefix: headsPrefix,
	}
}

// LocksHeads loads the current heads after locking. It will lock until the
// lock can be obtained.
func (hh *heads) LockHeads() error {
	hh.glock.Lock()
	err := hh.load()
	if err != nil {
		hh.glock.Unlock()
	}
	return nil
}

// UnlockHeads writes the head replacements. It then releases the lock.
func (hh *heads) UnlockHeads() error {
	defer hh.glock.Unlock()
	return hh.write()
}

func (hh *heads) key(c cid.Cid) ds.Key {
	// /<prefix>/<cid>
	return hh.prefix.Child(dshelp.CidToDsKey(c))
}

func (hh *heads) load() error {
	topic := ds.NewKey(hh.topic)
	q := query.Query{
		Prefix:   hh.prefix.Child(topic.Child(headsKey)).String(),
		KeysOnly: true,
	}

	results, err := hh.store.Query(q)
	if err != nil {
		return nil, err
	}
	defer results.Close()

	hh.heads = make(map[cid.Cid]cid.Cid)
	for r := range results.Next() {
		headCid, err := cidFromResult(r)
		if err != nil {
			logger.Errorf("error parsing head cid: %s", err)
			return nil, err
		}
		hh.heads[headCid] = cid.Undef
	}
	return nil
}

func (hh *heads) write() error {
	store := hh.store
	batchingDs, batching := store.(ds.Batching)
	if batching {
		store = batchingDs.Batch()
	}

	added, removed := hh.Diff()
	for _, add := range added {
		k := hh.key(add)
		err := hh.store.Put(key, nil)
		if err != nil {
			return err
		}
	}

	for _, rm := range removed {
		k := hh.key(rm)
		err := hh.store.Delete(key)
		if err != nil && err != ds.ErrNotFound {
			return err
		}
		if err == ds.ErrNotFound {
			logger.Warning("tried to remove unexisting head.")
		}
	}

	if batching {
		err := store.(ds.Batch).Commit()
		if err != nil {
			return err
		}
	}
	hh.heads == nil
	return nil
}

// IsHead returns if a given cid is among the current heads.
func (hh *heads) IsHead(c cid.Cid) bool {
	if hh.heads == nil { // maybe unnecessary
		logger.Warning("no heads loaded")
		return false
	}
	hh.mux.RLock()
	defer hh.mux.RUnlock()
	_, ok := hh.heads[c]
	return ok
}

func (hh *heads) Len() int {
	hh.mux.RLock()
	defer hh.mux.RUnlock()
	// Len of a map is not thread safe
	return len(hh.heads)
}

// Replace replaces a head with a new cid.
func (hh *heads) Replace(h, c cid.Cid) {
	if hh.heads == nil {
		logger.Warning("no heads loaded")
		return
	}
	hh.mux.Lock()
	defer hh.mux.Unlock()
	hh.heads[h] = c
}

func (hh *heads) Add(c cid.Cid) {
	hh.Replace(c, c)
}

// Diff returns the new added heads and the removed heads (those that have
// been replaced).
func (hh *heads) Diff() (added, removed []cid.Cid) {
	if hh.heads == nil {
		logger.Warning("no heads loaded")
		return nil, nil
	}
	hh.mux.RLock()
	defer hh.mux.RUnlock()
	for k, v := range hh.heads {
		if v != cid.Undef {
			added = append(added, v)
			if !v.Equals(k) {
				// equal k,v used to signal
				// newly added heads.
				removed = append(removed, k)
			}
		}
	}
}
