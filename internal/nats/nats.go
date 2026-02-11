package nats

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dnstapir/observation-encoder/internal/common"
)

const c_NATS_WILDCARD = common.NATS_WILDCARD
const c_NATS_GLOB = common.NATS_GLOB
const c_NATS_DELIM = common.NATS_DELIM
const c_MIN_RAW_KEY_LEN = 2 /* At least one flag label and one DNS label (not counting prefix) */

type Conf struct {
	Url               string `toml:"url"`
	Debug             bool   `toml:"debug"`
	Bucket            string `toml:"bucket"`
	SubjectPrefix     string `toml:"subject_prefix"`
	SubjectSouthbound string `toml:"subject_southbound"`
	Ttl               int    `toml:"ttl"`
	Log               common.Logger
}

type natsClient struct {
	log               common.Logger
	url               string
	queue             string
	bucket            string
	subjectPrefix     string
	subjectSouthbound string
	ttl               time.Duration
	kv                jetstream.KeyValue
	conn              *nats.Conn
}

func Create(conf Conf) (*natsClient, error) {
	nc := new(natsClient)

	if conf.Log == nil {
		return nil, errors.New("nil logger")
	}
	nc.log = conf.Log

	if conf.Url == "" {
		return nil, errors.New("no NATS URL")
	}

	if conf.Bucket == "" {
		return nil, errors.New("no bucket name")
	}

	if conf.SubjectPrefix == "" {
		return nil, errors.New("no subject prefix")
	}

	if conf.SubjectSouthbound == "" {
		return nil, errors.New("no southbound subject")
	}

	if conf.Ttl <= 0 {
		return nil, errors.New("zero ttl")
	}

    nc.url = conf.Url
	nc.bucket = conf.Bucket
	nc.subjectPrefix = strings.Trim(conf.SubjectPrefix, c_NATS_DELIM)
	nc.subjectSouthbound = strings.Trim(conf.SubjectSouthbound, c_NATS_DELIM)
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

	return strings.Trim(subjectCut, c_NATS_DELIM)
}

func (nc *natsClient) WatchObservations(ctx context.Context) (<-chan common.NatsMsg, error) {
	subjectParts := []string{nc.subjectPrefix, c_NATS_GLOB}
	subject := strings.Join(subjectParts, c_NATS_DELIM)
	w, err := nc.kv.Watch(ctx, subject, jetstream.UpdatesOnly())
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
			natsMsg := common.NatsMsg{
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
    subject := nc.genKeyFilterSubject(domain)
	ls, err := nc.kv.ListKeysFiltered(ctx, subject)
	if err != nil {
		nc.log.Error("Couldn't list keys for %s: %s", domain, err)
		return 0, err
	}

	var obs uint32
	for k := range ls.Keys() {
        flagUint, err := nc.extractObservationFromKey(k)
        if err != nil {
            nc.log.Warning("Couldn't extract observation: %s", err)
            continue
        }
		obs |= flagUint
	}

	return obs, nil
}

func (nc *natsClient) genKeyFilterSubject(domain string) string {
	domSplit := strings.Split(strings.Trim(domain, c_NATS_DELIM), c_NATS_DELIM)
	slices.Reverse(domSplit)
	domRev := strings.Join(domSplit, c_NATS_DELIM)

	subjectParts := []string{nc.subjectPrefix, c_NATS_WILDCARD, domRev}
	subject := strings.Join(subjectParts, c_NATS_DELIM)

    return subject
}

func (nc *natsClient) extractObservationFromKey(key string) (uint32, error) {
		kSplit := strings.Split(key, c_NATS_DELIM)
        prefixLen := len(strings.Split(nc.subjectPrefix, c_NATS_DELIM))

		if len(kSplit)-prefixLen < c_MIN_RAW_KEY_LEN {
			nc.log.Error("Badly formatted key '%s'", key)
            return 0, common.ErrBadKey
		}

		flag := kSplit[prefixLen] /* Flag is first label after prefix */
		flagUint, ok := common.OBS_MAP[flag]
		if !ok {
			nc.log.Error("Unrecognized flag '%s'", flag)
            return 0, common.ErrBadFlag
		}

        return flagUint, nil
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
			TTL:            nc.ttl,
			LimitMarkerTTL: nc.ttl,
		})
	if err != nil {
		nc.log.Error("Error creating key value store in NATS: %s", err)
		return err
	}

	nc.kv = kv
	nc.conn = conn
	nc.log.Debug("Nats key value store created successfully!")

	return nil
}

func (nc *natsClient) SendSouthboundObservation(msg string) error {
	// TODO really re-use connection to KV?
	outMsg := []byte(msg)
	err := nc.conn.Publish(nc.subjectSouthbound, outMsg)
	if err != nil {
		nc.log.Error("Couldn't publish %d bytes msg on %s", len(outMsg), nc.subjectSouthbound)
		return err
	} else {
		nc.log.Debug("Successful publish on '%s'", nc.subjectSouthbound)
	}

	return nil
}

func (nc *natsClient) Shutdown() error {
	// TODO impl
	return nil
}
