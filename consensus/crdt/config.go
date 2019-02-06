package crdt

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ipfs/ipfs-cluster/config"
)

var configKey = "crdt"

var (
	DefaultTopics        = []string{"ipfs"}
	DefaultPeersetMetric = "ping"
	DefaultMaxHeads      = 10
)

type Config struct {
	config.Saver

	hostShutdown bool

	// The name of the metric we use to obtain the peerset (every peer
	// with valid metric of this type is part of it).
	PeersetMetric string

	// The topics we wish to subscribe to
	Topics []string

	// How many different diverging heads we track before we
	// trigger the commit of an empty block.
	MaxHeads int
}

type jsonConfig struct {
	Topics        []string `json:"topics"`
	PeersetMetric string   `json:"peerset_metric,omitempty"`
}

func (cfg *Config) ConfigKey() string {
	return configKey
}

func (cfg *Config) Validate() error {
	if cfg.PeersetMetric == "" {
		errors.New("crdt.PeersetMetric needs a name")
	}
}

func (cfg *Config) LoadJSON(raw []byte) error {
	jcfg := &jsonConfig{}
	err := json.Unmarshal(raw, jcfg)
	if err != nil {
		return fmt.Errorf("error unmarshaling %s config", configKey)
	}

	cfg.Default()

	cfg.Topics = jcfg.Topics
	config.SetIfNotDefault(jcfg.PeersetMetric, &cfg.PeersetMetric)
	return cfg.Validate()
}

func (cfg *Config) ToJSON() ([]byte, error) {
	jcfg := &jsonConfig{
		Topics:        cfg.Topics,
		PeersetMetric: "",
	}

	if cfg.PeersetMetric != DefaultPeersetMetric {
		jcfg.PeersetMetric = cfg.PeersetMetric
		// otherwise leave empty/hidden
	}

	return config.DefaultJSONMarshal(jcfg)
}

func (cfg *Config) Default() error {
	cfg.Topics = DefaultTopics
	cfg.PeersetMetric = DefaultPeersetMetric
	return nil
}
