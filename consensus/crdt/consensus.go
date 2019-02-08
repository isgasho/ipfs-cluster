package crdt

import (
	"context"

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

	state     state.State
	store     ds.Datastore
	namespace ds.Key

	pubsub       *pubsub.Pubsub
	subscription *pubsub.Subscription

	rpcClient *rpc.Client
	rpcReady  chan struct{}
	readyCh   chan struct{}

	shutdownLock sync.RWMutex
	shutdown     bool
}

func New(
	host host.Host,
	cfg *Config,
	state state.State,
	store ds.Datastore,
	namespace ds.Key,
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

	css := &Consensus{
		ctx:       ctx,
		cancel:    cancel,
		host:      host,
		state:     state,
		store:     store,
		namespace: namespace,
		pubsub:    gossip,
		rpcReady:  make(chan struct{}, 1),
		readyCh:   make(chan struct{}, 1),
	}

	go css.setup()
	return css, nil
}

func (css *Consensus) setup() {
	select {
	case <-css.ctx.Done():
		return
	case <-css.rpcReady:
	}

	topicSub, err := gossip.Subscribe(cfg.ClusterName)
	if err != nil {
		logger.Errorf("error subscribing to topic: %s", err)
		return
	}
	css.subscription = topicSub

	crdt, err := newCRDT(
		css.state,
		css.store,
		css.namespace.ChildString(cfg.ClusterName),
		&crdtDAGService{css.rpcClient},
		css.publish,
	)

	if err != nil {
		topicSub.Cancel()
		logger.Errorf("error setting up CRDT: %s", err)
		return
	}

	go css.handleSub()
	cc.readyCh <- struct{}{}
}

func (css *Consensus) handleSub() {
	sub := css.subscription
	for {
		msg, err := sub.Next()
		if err != nil {
			logger.Warning(err)
			// TODO check loglevel when I know how a subscription
			// cancelled error looks like.
			return
		}

		fromBytes := msg.GetFrom()
		pid, err := peer.IDB58Decode(fromBytes)
		if err != nil {
			logger.Error(err)
			continue
		}

		if !css.isPeerTrusted(msg.Peer) {
			// ignore messages published from untrusted peers
			logger.Debug("ignoring message from untrusted peer ")
			continue
		}

		c, err := cid.Cast(msg.GetData())
		if err != nil {
			logger.Error(err)
			continue
		}

		// Check for custom cluster codec in CID ?
		err := css.crdt.HandleBlock(context.TODO(), c)
		if err != nil {
			logger.Error("error handling block: %s", err)
		}
	}
}

func (css *Consensus) publish(c cid.Cid) error {
	return css.subscription.Publish(css.cfg.ClusterName, c.Bytes())
}

func (css *Consensus) Shutdown() error {
	cc.shutdownLock.Lock()
	defer cc.shutdownLock.Unlock()

	if cc.shutdown {
		logger.Debug("already shutdown")
		return nil
	}

	logger.Info("stopping Consensus component")

	if sub := css.subscription; sub != nil {
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
