package crdt

import (
	"context"
	"errors"
	"net/rpc"

	"github.com/gogo/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/ipfs-cluster/api"
)

// IPLD related types and implementations

const (
	ClusterCRDTPb   = 0xc101
	ClusterCRDTCbor = 0xc102
)

func init() {
	// ipld.Register(ClusterCRDTCbor, cbor.DecodeBlock) // need to decode CBOR
	ipld.Register(ClusterCRDTPb, dag.DecodeProtoBufBlock)
}

const defaultWorkers = 20

// crdtDAGService implements a ipld.DAGService (pooled for GetMany) along with
// additonal methods to handle CRDT blocks
type crdtDAGService struct {
	rpcClient *rpc.Client
	workers   int
}

func (ng *crdtDAGService) Get(ctx context.Context, c cid.Cid) (ipld.Node, error) {
	var blockBytes []byte

	err := css.rpcClient.CallContext(
		ctx,
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

func (ng *crdtDAGService) GetMany(ctx context.Context, cids []cid.Cid) <-chan *ipld.NodeOption {
	results := make(chan *ipld.NodeOption, len(cids))
	jobs := make(chan cid.Cid, len(cids))

	if ng.workers <= 0 {
		ng.workers = defaultWorkers
	}

	// launch maximum ng.workers. Do not launch more workers than cids
	// though.
	for w := 0; w < ng.workers && w < len(cids); w++ {
		go func() {
			for c := range jobs {
				nd, err := ng.Get(ctx, c)
				results <- &ipld.NodeOption{
					Node:  nd,
					Error: err,
				}
			}
		}()
	}

	for _, c := range cids {
		jobs <- c
	}
	close(jobs)
}

func (ng *crdtDAGService) GetDelta(ctx context.Context, c cid.Cid) (ipld.Node, *pb.Delta, error) {
	nd, err := ng.GetBlock(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	return nd, extractDelta(nd), nil
}

// GetHeight returns the height of a block
func (ng *crdtDAGService) GetHeight(ctx context.Context, c cid.Cid) (uint64, error) {
	_, delta, err := ng.GetDelta(ctx, c)
	if err != nil {
		return 0, err
	}
	return delta.Height, nil
}

func extractDelta(nd *ipld.Node) (*pb.Delta, error) {
	ci := nd.Cid()
	switch ci.Prefix().Codec {
	case ClusterCRDTPb:
		protonode, ok := nd.(*dag.ProtoNode)
		if !ok {
			return nil, errors.New("node is not a ProtoNode")
		}
		d := &pb.Delta{}
		err := proto.Unmarshal(protonode.Data(), d)
		return d, err
	case ClusterCRDTCbor:
		return nil, errors.New("CBOR not supported")
	default:
		return nil, errors.New("unknown CID codec")
	}
}
