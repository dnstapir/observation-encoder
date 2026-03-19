package app

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/dnstapir/tapir-analyse-lib/common"
)

const c_N_HANDLERS = 3
const c_NATS_DELIM = common.NATS_DELIM

type Conf struct {
	Debug          bool `toml:"debug"`
	TtlMargin      int  `toml:"ttl_margin"`
	Log            common.Logger
	NatsHandle     nats
	LibtapirHandle libtapir
}

type appHandle struct {
	id             string
	ttlMargin      int
	log            common.Logger
	natsHandle     nats
	libtapirHandle libtapir
	exitCh         chan<- common.Exit
	pm
}

type pm struct {
	natsInCount atomic.Int64
}

type job struct {
	msg common.NatsMsg
}

type nats interface {
	WatchObservations(context.Context) (<-chan common.NatsMsg, error)
	RemovePrefix(string) string
	GetObservations(context.Context, string) (uint32, int, error)
	SendSouthboundObservation(string) error
	Shutdown() error
}

type libtapir interface {
	GenerateObservationMsg(string, uint32, int) (string, error) // TODO set ttl?
}

func Create(conf Conf) (*appHandle, error) {
	a := new(appHandle)

	if conf.Log == nil {
		return nil, common.ErrBadHandle
	}

	if conf.NatsHandle == nil {
		return nil, common.ErrBadHandle
	}

	if conf.LibtapirHandle == nil {
		return nil, common.ErrBadHandle
	}

	// TODO what is a reasonale safety margin (in seconds) for adding to outgoing TTL?
	if conf.TtlMargin < 0 || conf.TtlMargin > 3600 {
		return nil, common.ErrBadParam
	}

	a.log = conf.Log
	a.id = "main app"
	a.ttlMargin = conf.TtlMargin
	a.natsHandle = conf.NatsHandle
	a.libtapirHandle = conf.LibtapirHandle

	return a, nil
}

func (a *appHandle) Run(ctx context.Context, exitCh chan<- common.Exit) {
	a.id = "main app"
	a.exitCh = exitCh
	jobChan := make(chan job, 10)

	natsInCh, err := a.natsHandle.WatchObservations(ctx)
	if err != nil {
		a.log.Error("Error connecting to NATS: %s", err)
		a.exitCh <- common.Exit{ID: a.id, Err: err}
		return
	}

	var wg sync.WaitGroup
	for range c_N_HANDLERS {
		wg.Go(func() {
			for j := range jobChan {
				a.handleJob(ctx, j)
			}
			a.log.Info("Worker done!")
		})
	}

MAIN_APP_LOOP:
	for {
		select {
		case msg, ok := <-natsInCh:
			if !ok {
				a.log.Warning("NATS channel closed, exiting main loop")
				break MAIN_APP_LOOP
			}
			a.pm.natsInCount.Add(1)
			j := job{
				msg: msg,
			}
			jobChan <- j
		case <-ctx.Done():
			a.log.Info("Stopping main worker thread")
			break MAIN_APP_LOOP
		}
	}

	close(jobChan)

	wg.Wait()

	err = a.natsHandle.Shutdown()
	if err != nil {
		a.log.Error("Encountered '%s' during NATS shutdown", err)
	}

	a.exitCh <- common.Exit{ID: a.id, Err: err}
	a.log.Info("Main app shutdown done")
	return
}

func (a *appHandle) handleJob(ctx context.Context, j job) {
	a.log.Info("Got message on subject '%s'", j.msg.Subject)

	domainRev := a.natsHandle.RemovePrefix(j.msg.Subject)
	domainSplit := strings.Split(domainRev, c_NATS_DELIM)

	if len(domainSplit) < 2 { /* at least one observation type and one DNS label */
		a.log.Warning("Incoming message subject %s has too few labels, ignoring...", j.msg.Subject)
		return
	}

	slices.Reverse(domainSplit)

	/* After reverse, observation type is at the end. Drop it, we just want the domain name */
	domain := strings.Join(domainSplit[:len(domainSplit)-1], c_NATS_DELIM)

	a.log.Debug("Extracted domain '%s'", domain)

	obs, ttl, err := a.natsHandle.GetObservations(ctx, domain)
	if err != nil {
		a.log.Error("Could not get observations for %s: %s", domain, err)
		return
	}

	if obs == 0 || ttl == 0 {
		a.log.Debug("Observation for %s has value %d and ttl %d, won't bother sending", domain, obs, ttl)
		return
	}

	a.log.Debug("%s has observation vector %d", domain, obs)

	obsJSON, err := a.libtapirHandle.GenerateObservationMsg(domain, obs, int(ttl)+a.ttlMargin)
	if err != nil {
		a.log.Error("Couldn't generate JSON observation: %s", err)
		return
	}

	err = a.natsHandle.SendSouthboundObservation(obsJSON)
	if err != nil {
		a.log.Error("Couldn't send southbound observation: %s", err)
	}

	a.log.Debug("Done handling msg on subject %s", j.msg.Subject)

	return
}

func (a *appHandle) GetNatsInCount() int64 {
	return a.pm.natsInCount.Load()
}
