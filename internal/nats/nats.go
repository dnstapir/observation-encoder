package nats

import (
	"context"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dnstapir/observation-encoder/internal/common"
)

const c_BUCKET_NAME = "obs_bucket" // TODO make configurable
const c_DEFAULT_TTL = 60             /* seconds */ // TODO make configurable

type Conf struct {
    Url        string `toml:"url"`
	OutSubject string `toml:"out_subject"`
	WatchSubject  string `toml:"watch_subject"`
	Debug         bool `toml:"debug"`
	Log        common.Logger
}

type natsClient struct {
	log        common.Logger
	url        string
	outSubject string
	watchSubject  string
	queue      string
	kv         jetstream.KeyValue
}

func Create(conf Conf) (*natsClient, error) {
	nc := new(natsClient)

	nc.url = conf.Url // TODO validate

	if conf.Log == nil {
		return nil, errors.New("nil logger")
	}
	nc.log = conf.Log

	if conf.OutSubject == "" || conf.WatchSubject == "" {
		return nil, errors.New("bad nats subject")
	}
	nc.outSubject = conf.OutSubject
	nc.watchSubject = conf.WatchSubject

	err := nc.initNats()
	if err != nil {
		nc.log.Error("Error initializing NATS")
		return nil, err
	}

	return nc, nil
}

func (nc *natsClient) WatchBucket(ctx context.Context) (<-chan common.NatsMsg, error) {
    w, err := nc.kv.Watch(ctx, nc.watchSubject)
	if err != nil {
        nc.log.Error("Couldn't watch '%s': %s", nc.watchSubject, err)
		return nil, err
	}

	outCh := make(chan common.NatsMsg)
	go func() {
		nc.log.Info("Starting NATS listener loop")
		for val := range w.Updates() {
            if val == nil {
                continue
            }
			nc.log.Debug("Incoming NATS KV update on '%s'!", val.Key())
			natsMsg := common.NatsMsg {
				Headers: nil,
				Data:    val.Value(),
                Subject: val.Key(),
			}
			outCh <- natsMsg
		}
		close(outCh)
	}()

	nc.log.Info("Watching subject '%s'", nc.watchSubject)

	return outCh, nil
}

func (nc *natsClient) initNats() error {
	conn, err := nats.Connect(nc.url)
	if err != nil {
		nc.log.Error("Error connecting to nats while setting up KV store: %s", err)
		return err
	}
	js, err := jetstream.New(conn)
	if err != nil {
		nc.log.Error("Error creating jetstream handle: %s", err)
		return err
	}

	ctx := context.Background()

	kv, err := js.CreateKeyValue(ctx, // TODO let someone else provision this resource
		jetstream.KeyValueConfig{
			Bucket:         c_BUCKET_NAME,
			LimitMarkerTTL: c_DEFAULT_TTL * time.Second, // TODO what is a good setting?
		})
	if err != nil {
		nc.log.Error("Error creating key value store in NATS: %s", err)
		return err
	}

	nc.kv = kv
	nc.log.Debug("Nats key value store created successfully!")

	return nil
}

func (nc *natsClient) Shutdown() error {
    // TODO impl
    return nil
}
