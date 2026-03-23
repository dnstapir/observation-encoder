package nats

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/dnstapir/tapir-analyse-lib/common"
)

const c_NATS_WILDCARD = common.NATS_WILDCARD
const c_NATS_GLOB = common.NATS_GLOB
const c_NATS_DELIM = common.NATS_DELIM
const c_MIN_RAW_KEY_LEN = 2 /* At least one flag label and one DNS label (not counting prefix) */

type Conf struct {
	Debug                    bool         `toml:"debug"`
	Url                      string       `toml:"url"`
	SubjectSouthbound        string       `toml:"subject_southbound"`
	ObservationSubjectPrefix string       `toml:"observation_subject_prefix"`
	Buckets                  []BucketConf `toml:"buckets"`
	Log                      common.Logger
}

type BucketConf struct {
	Name string `toml:"name"`
	Ttl  int    `toml:"ttl"`
}

type natsClient struct {
	url                      string
	subjectSouthbound        string
	observationSubjectPrefix string
	kvs                      []jetstream.KeyValue
	conn                     *nats.Conn
	log                      common.Logger
	wg                       sync.WaitGroup
}

func Create(conf Conf) (*natsClient, error) {
	nc := new(natsClient)

	if conf.Log == nil {
		return nil, errors.New("nil logger")
	}
	nc.log = conf.Log

	if conf.Url == "" {
		return nil, errors.New("no nats url")
	}
	nc.url = conf.Url

	if len(conf.Buckets) == 0 {
		return nil, errors.New("no buckets")
	}

	if conf.SubjectSouthbound == "" {
		return nil, errors.New("no southbound subject")
	}
	nc.subjectSouthbound = strings.Trim(conf.SubjectSouthbound, c_NATS_DELIM)

	if conf.ObservationSubjectPrefix == "" {
		return nil, errors.New("no observation subject prefix")
	}
	nc.observationSubjectPrefix = strings.Trim(conf.ObservationSubjectPrefix, c_NATS_DELIM)

	err := nc.initNats(conf.Buckets)
	if err != nil {
		nc.log.Error("Error initializing NATS")
		return nil, err
	}

	return nc, nil
}

func (nc *natsClient) RemovePrefix(subject string) string {
	subjectCut, ok := strings.CutPrefix(subject, nc.observationSubjectPrefix)
	if !ok {
		nc.log.Warning("Subject '%s' missing prefix '%s'", subject, nc.observationSubjectPrefix)
	}

	return strings.Trim(subjectCut, c_NATS_DELIM)
}

func (nc *natsClient) WatchObservations(ctx context.Context) (<-chan common.NatsMsg, error) {
	aggrKeyChan := make(chan jetstream.KeyValueEntry, 128) // TODO adjustable buffer size?
	outCh := make(chan common.NatsMsg, 32)                 // TODO adjustable buffer size?
	atLeastOneBucket := false

	for _, kv := range nc.kvs {
		subjectParts := []string{nc.observationSubjectPrefix, c_NATS_GLOB}
		subject := strings.Join(subjectParts, c_NATS_DELIM)
		w, err := kv.Watch(ctx, subject, jetstream.UpdatesOnly())
		if err != nil {
			nc.log.Error("Couldn't watch bucket %s: '%s'. Skipping...", kv.Bucket(), err)
			continue
		} else {
			atLeastOneBucket = true
		}

		nc.wg.Go(func() {
			for k := range w.Updates() {
				select {
				case aggrKeyChan <- k:
				case <-ctx.Done():
					return
				}
			}
			nc.log.Info("Watcher for bucket %s stopping", kv.Bucket())
		})

		nc.log.Info("Watching bucket '%s'", kv.Bucket())
	}

	nc.wg.Go(func() {
		defer close(outCh)
		defer nc.log.Info("Leaving aggregate NATS listener loop")
		nc.log.Info("Starting aggregate NATS listener loop")
		for {
			select {
			case <-ctx.Done():
				return
			case val, ok := <-aggrKeyChan:
				if !ok {
					nc.log.Warning("Aggregate NATS channel closed unexpectedly")
					return
				}
				if val == nil {
					continue
				}
				nc.log.Debug("Incoming NATS KV update on '%s'!", val.Key())
				natsMsg := common.NatsMsg{
					Headers: nil,
					Data:    val.Value(),
					Subject: val.Key(),
				}
				select {
				case outCh <- natsMsg:
				case <-ctx.Done():
					return
				}
			}
		}
	})

	if !atLeastOneBucket {
		return nil, errors.New("failed to watch any buckets")
	}

	return outCh, nil
}

func (nc *natsClient) GetObservations(ctx context.Context, domain string) (uint32, int, error) {
	var obs uint32
	ttl := math.MaxInt64
	subject := nc.genKeyFilterSubject(domain)

	for _, kv := range nc.kvs {
		ls, err := kv.ListKeysFiltered(ctx, subject)
		if err != nil {
			nc.log.Error("Couldn't list keys for %s in bucket %s: %s", domain, kv.Bucket(), err)
			return 0, 0, err
		}

		status, err := kv.Status(ctx)
		if err != nil {
			nc.log.Error("Couldn't get bucket status for %s: %s", kv.Bucket(), err)
			continue
		}
		ttlBucket := status.TTL()
		for k := range ls.Keys() {
			flagUint, err := nc.extractObservationFromKey(k)
			if err != nil {
				nc.log.Warning("Couldn't extract observation: %s", err)
				continue
			}

			val, err := kv.Get(ctx, k)
			if err != nil {
				nc.log.Error("Couldn't get status for key %s: %s", k, err)
				continue
			}

			keyAge := time.Now().Sub(val.Created())
			keyTtl := int(math.Round((ttlBucket - keyAge).Seconds()))
			if keyTtl <= 0 {
				nc.log.Warning("Bucket %s seems to contain outdated key %s", kv.Bucket(), k)
				continue
			}

			obs |= flagUint
			if keyTtl < ttl {
				ttl = keyTtl
			}
		}
	}

	/* If not updated, set to zero because something probably went bad and results are unreliable */
	if ttl == math.MaxInt64 {
		ttl = 0
		obs = 0
	}

	return obs, ttl, nil
}

func (nc *natsClient) genKeyFilterSubject(domain string) string {
	domSplit := strings.Split(strings.Trim(domain, c_NATS_DELIM), c_NATS_DELIM)
	slices.Reverse(domSplit)
	domRev := strings.Join(domSplit, c_NATS_DELIM)

	subjectParts := []string{nc.observationSubjectPrefix, c_NATS_WILDCARD, domRev}
	subject := strings.Join(subjectParts, c_NATS_DELIM)

	return subject
}

func (nc *natsClient) extractObservationFromKey(key string) (uint32, error) {
	kSplit := strings.Split(key, c_NATS_DELIM)
	prefixLen := len(strings.Split(nc.observationSubjectPrefix, c_NATS_DELIM))

	if len(kSplit)-prefixLen < c_MIN_RAW_KEY_LEN {
		nc.log.Error("Badly formatted key '%s'", key)
		return 0, common.ErrBadKey
	}

	flag := kSplit[prefixLen] /* Flag is first label after prefix */
	obsEnc, ok := common.OBS_MAP[flag]
	if !ok {
		nc.log.Error("Unrecognized flag '%s'", flag)
		return 0, common.ErrBadFlag
	}

	return obsEnc, nil
}

func (nc *natsClient) initNats(buckets []BucketConf) error {
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	nc.kvs = make([]jetstream.KeyValue, 0)
	for _, b := range buckets {
		if b.Name == "" {
			nc.log.Error("Attempting to create bucket with no name. Skipping...")
			continue
		}
		if b.Ttl <= 0 {
			nc.log.Error("Attempting to create bucket with invalid TTL. Skipping...")
			continue
		}

		kv, err := js.CreateKeyValue(ctx,
			jetstream.KeyValueConfig{
				Bucket:         b.Name,
				TTL:            time.Duration(b.Ttl) * time.Second,
				LimitMarkerTTL: time.Duration(1) * time.Second,
				Description:    fmt.Sprintf("TTL: %d seconds", b.Ttl),
			})
		if err != nil {
			nc.log.Error("Error creating key value store in NATS: %s. Skipping...", err)
			continue
		}

		nc.kvs = append(nc.kvs, kv)
	}

	if len(nc.kvs) == 0 {
		nc.log.Error("Failed to create any buckets")
		return errors.New("no buckets created")
	}

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
	nc.wg.Wait()
	// TODO NATS shutdown
	return nil
}
