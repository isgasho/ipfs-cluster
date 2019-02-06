package crdt

import (
	"context"
	"errors"

	"net/rpc"
	"sync"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/ipfs-cluster/api"
	"github.com/ipfs/ipfs-cluster/state"

	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log"
	host "github.com/libp2p/go-libp2p-host"
	peer "github.com/libp2p/go-libp2p-peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

var logger = logging.Logger("crdt")

type Consensus struct {
	ctx    context.Context
	cancel context.CancelFunc

	host host.Host

	state      state.State
	store      ds.Datastore
	namespace  ds.Key
	dSets      map[string]*crdtSet
	heads      map[string]*heads
	topicChans map[string]chan *message
	pSub       *pubsub.Pubsub
	subs       []*pubsub.Subscription

	rpcClient *rpc.Client
	rpcReady  chan struct{}
	readyCh   chan struct{}

	shutdownLock sync.RWMutex
	shutdown     bool
}

type message struct {
	block cid.Cid
	topic string
	peer  peer.ID
}

func New(
	host host.Host,
	cfg *Config,
	state state.State,
	store ds.Datastore,
	namespace string,
) (*Consensus, err) {
	err := cfg.Validate()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	gossip, err := pubsub.NewGossipPub(
		ctx,
		host,
		pubsub.WithMessageSigning(true),
		pubsub.WithSignatureVerification(true),
		pubsub.WithStrictSignatureVerification(true),
	)

	prefix := ds.NewKey(namespace)
	lenTopics := len(cfg.Topics)
	subs := make([]*pubsub.Subscription, lenTopics, lenTopics)
	dsets := make(map[string]*crdtSet, lenTopics)
	heads := make(map[string]*heads, lenTopics)
	chans := make(map[string]chan *message, lenTopics)

	css := &Consensus{
		ctx:        ctx,
		cancel:     cancel,
		host:       host,
		state:      state,
		store:      store,
		namespace:  prefix,
		dSets:      dsets,
		heads:      heads,
		topicChans: chans,
		pSub:       gossip,
		subs:       subs,
		rpcReady:   make(chan struct{}, 1),
		readyCh:    make(chan struct{}, 1),
	}

	for i, t := range cfg.Topics {
		subs[i], err = gossip.Subscribe(t)
		if err != nil {
			return nil, err
		}

		// <namespace>/<topic>/set
		topicSetNs := prefix.ChildString(topic).ChildString(setNs)
		// <namespace>/<topic>/heads
		topicHeadsNs := prefix.ChildString(topic).ChildString(headsNs)
		// <namespace>/<topic>/blocks
		topicBlocksNs := prefix.ChildString(topic).ChildString(blocksNs)

		dsets[t] = newCRDTSet(store, topicSetNs)
		heads[t] = newHeads(store, topicHeadsNs)
		blocks[t] = newBlocks(store, topicBlocksNs)
		chans[t] = make(chan *message, messageBufferLen)
		go css.handleMessages(t)
	}

	go css.handleSubs()

	return css, nil
}

func (css *Consensus) Shutdown() error {
	cc.shutdownLock.Lock()
	defer cc.shutdownLock.Unlock()

	if cc.shutdown {
		logger.Debug("already shutdown")
		return nil
	}

	logger.Info("stopping Consensus component")

	// Cancel subscriptions
	for _, sub := range css.subs {
		sub.Cancel()
	}

	if css.config.hostShutdown {
		css.host.Close()
	}

	css.shutdown = true
	css.cancel()
	close(cc.rpcReady)
	return nil
}

func (css *Consensus) SetClient(c *rpc.Client) {
	css.rpcClient = c
	css.rpcReady <- struct{}{}
}

func (css *Consensus) Ready() <-chan struct{} {
	return cc.readyCh
}

func (css *Consensus) LogPin(pin api.Pin) error {

}

func (css *Consensus) LogUnpin(pin api.Pin) error {

}

// Peers returns the current known peerset. It uses
// the monitor component and considers every peer with
// valid known metrics a memeber.
func (css *Consensus) Peers() ([]peer.ID, error) {
	var metrics []api.Metrics

	err := cc.rpcClient.CallContext(
		css.ctx,
		"",
		"Cluster",
		"PeerMonitorLatestMetrics",
		css.cfg.PeersetMetric,
		&metrics,
	)
	if err != nil {
		return nil, err
	}

	peers := make([]peer.ID, len(metrics), len(metrics))

	for i, m := range metrics {
		peers[i] = metrics.Peer
	}
	return peers, nil
}

func (css *Consensus) AddPeer(pid peer.ID) error { return nil }

func (css *Consensus) RmPeer(pid peer.ID) error { return nil }

func (css *Consensus) State() (state.State, error) { return css.state, nil }

func (css *Consensus) Clean() error { return nil }

func (css *Consensus) Rollback(state state.State) error {
	return nil
}

func (css *Consensus) handleSubs() {
	select {
	case <-cc.ctx.Done():
		return
	case <-cc.rpcReady:
	}

	for _, sub := range css.subs {
		go css.handleSub(sub)
	}

	cc.readyCh <- struct{}{}
}

func (css *Consensus) handleSub(sub *pubsub.Subscription) {
	topic := sub.Topic()
	ch, ok := css.topicChans[topic]
	if !ok {
		panic("need a channel for each subscription. bug!")
	}

	for {
		msg, err := sub.Next()
		if err != nil {
			return
		}

		fromBytes := msg.GetFrom()
		pid, err := peer.IDB58Decode(fromBytes)
		if err != nil {
			logger.Error(err)
			continue
		}

		c, err := cid.Decode(msg.GetData())
		if err != nil {
			logger.Error(err)
			continue
		}

		if !css.isPeerTrusted(msg.Peer) {
			// ignore messages published from untrusted peers
			logger.Debug("ignoring message from untrusted peer ")
			continue
		}

		known, err := css.isKnownBlock(topic, msg.block)
		if err != nil {
			logger.Errorf("error checking if block is known: %s", err)
			continue
		}
		if known {
			// we already processed this block
			continue
		}

		// We need to process this block. Send it to the worker
		// for this topic (handleMessages). This allows to use the
		// channel for buffering.
		select {
		case ch <- &message{c, topic, pid}:
			// default: // do not lock if processing message
		}
	}
}

// handleMessages runs in a goroutine which processes a block received
// via pubsub. Only one tree can be processed at a time.
func (css *Consensus) handleMessages(topic string) {
	ch, ok := css.topicChans[topic]
	if !ok {
		panic("no channel for topic. Bug!")
	}

	errorf := func(msg string, err error) {
		logger.Errorf("error %s: %s", msg, err)
	}
	// handles one message at a time for each topic
	// downloads the missing tree
	// applies upgrades
	// updates heads
	for msg := range ch {
		hds, err := css.heads(topic)
		if err != nil {
			errorf("getting heads", err)
			continue
		}

		if heads.Len() == 0 {
			logger.Info("No current heads known. Syncing full tree from IPFS")
			err := css.fetchRefs(block, -1)
			if err != nil {
				errorf("fetching tree refs", err)
				continue
			}
		}

		err := css.walkAndApplyTree(topic, msg.block, msg.block, hds)
		if err != nil {
			errorf("walking tree", err)
		}
	}
}

// errors used for the walker
var (
	errHeadFound       = errors.New("head found")
	errKnownBlockFound = errors.New("known block found")
)

func (css *Consensus) walkAndApplyTree(
	topic string,
	rootCid cid.Cid,
	topBlock cid.Cid,
	hds *heads,
) error {

	ng := &nodeGetter{rpcClient: css.rpcClient}
	root, err := ng.Get(css.ctx, rootCid)
	if err != nil {
		return err
	}

	navRoot := ipld.NewNavigableIPLDNode(root, ng)
	wlkr := ipld.NewWalker(css.ctx, navNode)

	visitor := func(nnd ipld.NavigableNode) error {
		block := ipld.ExtractIPLDNode(nnd)
		if hds.IsHead(block.Cid()) {
			// we reached a current head. Replace it
			hds.Replace(block, topBlock)
			return ipld.ErrDownNoChild
		}

		if css.isKnownBlock(nd.Cid()) {
			// we reached a known block (not head)

			// this check should be redundant because we always call
			// walkAndApplyTree with a not-known block so this must be
			// different from the topBlock, but still...  if the topblock
			// is known, we should just ignore it.
			if !nd.Equals(topBlock) {
				// insert a new head
				hds.Replace(topBlock, topBlock)
			}
			return ipld.ErrDownNoChild
		}

		if err != nil {
			return err
		}

	}

	if css.isKnownBlock(block) {
		// we reached a known block (not head)

		// this check should be redundant because we always call
		// walkAndApplyTree with a not-known block so this must be
		// different from the topBlock, but still...  if the topblock
		// is known, we should just ignore it.
		if !block.Equals(topBlock) {
			// insert a new head
			hds.Replace(topBlock, topBlock)
		}
		return nil
	}

	nd, err := css.getBlock(block)
	if err != nil {
		return err
	}

}

func (css *Consensus) fetchRefs(c cid.Cid, depth int) error {
	// fetch all refs
	return css.rpcClient.CallContext(
		css.ctx,
		"",
		"Cluster",
		"IPFSConnectorFetchRefs",
		depth,
		&struct{}{},
	)
}

func (css *Consensus) getBlock(c cid.Cid) (ipld.Node, error) {
	var blockBytes []byte

	err := css.rpcClient.CallContext(
		css.ctx,
		"",
		"Cluster",
		"IPFSBlockGet",
		api.PinCid(c),
		&blockBytes,
	)
	if err != nil {
		return nil, err
	}

	bl, err := blocks.NewBlockWithCid(blockBytes, c)
	if err != nil {
		return nil, err
	}
	return ipld.Decode(bl)
}

func (css *Consensus) nodeGetter() ipld.NodeGetter {
	return &nodeGetter{
		rpcClient: css.rpcClient,
	}
}
