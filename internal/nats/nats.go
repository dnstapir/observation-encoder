package nats

import (
	"context"
	"errors"
	"time"
    "slices"
    "strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dnstapir/observation-encoder/internal/common"
)

const c_NATS_WILDCARD = common.NATS_WILDCARD
const c_NATS_GLOB = common.NATS_GLOB
const c_NATS_DELIM = common.NATS_DELIM

type Conf struct {
    Url           string `toml:"url"`
	Debug         bool   `toml:"debug"`
    Bucket        string `toml:"bucket"`
    SubjectPrefix string `toml:"subject_prefix"`
    Ttl           int    `toml:"ttl"`
	Log           common.Logger
}

type natsClient struct {
	log        common.Logger
	url        string
	queue      string
    bucket        string
    subjectPrefix string
    ttl           time.Duration
	kv         jetstream.KeyValue
}

func Create(conf Conf) (*natsClient, error) {
	nc := new(natsClient)

	nc.url = conf.Url // TODO validate

	if conf.Log == nil {
		return nil, errors.New("nil logger")
	}
	nc.log = conf.Log

    if conf.Bucket == "" {
        return nil, errors.New("no bucket name")
    }

    if conf.SubjectPrefix == "" {
        return nil, errors.New("no subject prefix")
    }

    if conf.Ttl == 0 {
        return nil, errors.New("zero ttl")
    }


    nc.bucket        = conf.Bucket
    nc.subjectPrefix = strings.Trim(conf.SubjectPrefix, c_NATS_DELIM)
    nc.ttl = time.Duration(conf.Ttl) * time.Second

	err := nc.initNats()
	if err != nil {
		nc.log.Error("Error initializing NATS")
		return nil, err
	}

	return nc, nil
}

func (nc *natsClient) RemovePrefix(subject string) string {
     subjectCut, ok := strings.CutPrefix(subject, nc.subjectPrefix)
     if !ok {
         nc.log.Warning("Subject '%s' missing prefix '%s'", subject, nc.subjectPrefix)
     }

     return subjectCut
}

func (nc *natsClient) WatchObservations(ctx context.Context) (<-chan common.NatsMsg, error) {
    subjectParts := []string{nc.subjectPrefix, c_NATS_GLOB}
    subject := strings.Join(subjectParts, c_NATS_DELIM)
    w, err := nc.kv.Watch(ctx, subject)
	if err != nil {
        nc.log.Error("Couldn't watch: %s", err)
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

	nc.log.Info("Watching subject '%s'", subject)

	return outCh, nil
}

func (nc *natsClient) GetObservations(ctx context.Context, domain string) (uint32, error) {
    domSplit := strings.Split(strings.Trim(domain, c_NATS_DELIM), c_NATS_DELIM)
    slices.Reverse(domSplit)
    domRev := strings.Join(domSplit, c_NATS_DELIM)

    subjectParts := []string{nc.subjectPrefix, c_NATS_WILDCARD, domRev}
    subject := strings.Join(subjectParts, c_NATS_DELIM)
    ls, err := nc.kv.ListKeysFiltered(ctx, subject)
    if err != nil {
        nc.log.Error("Couldn't list keys for %s: %s", domain, err)
        return 0, err
    }

    var obs uint32
    for k := range ls.Keys() {
        kSplit := strings.Split(k, c_NATS_DELIM)
        flag := kSplit[1] // TODO avoid magic values
        flagUint, ok := common.OBS_MAP[flag]
        if !ok {
            nc.log.Warning("Unrecognized flag '%s', ignoring...")
            continue
        }
        obs |= flagUint
    }

    return obs, nil
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
			Bucket:         nc.bucket,
			LimitMarkerTTL: nc.ttl,
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
